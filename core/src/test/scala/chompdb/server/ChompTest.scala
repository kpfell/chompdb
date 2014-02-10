package chompdb.server

import chompdb._
import chompdb.store._
import chompdb.testing._
import f1lesystem.{ FileSystem, LocalFileSystem }
import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }
import java.io.IOException
import java.io.{ ObjectInputStream, ObjectOutputStream }
import java.nio.ByteBuffer
import java.util.concurrent.{ ScheduledExecutorService, TimeUnit }
import scala.collection._
import scala.collection.mutable.SynchronizedSet
import scala.concurrent.duration._
import scala.reflect.runtime.universe._

import org.mockito.Mockito.{ mock, when }
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChompTest extends WordSpec with ShouldMatchers {
  import TestUtils.stringToByteArray
  import TestUtils.byteArrayToString
  import TestUtils.createEmptyShard

  val testName = "ChompTest"

  val tmpLocalRoot = LocalFileSystem.tempRoot("ChompTest") /+ "local"
  val tmpLocalRoot2 = LocalFileSystem.tempRoot("ChompGetterTest") /+ "local"
  val tmpRemoteRoot = LocalFileSystem.tempRoot("ChompTest") /+ "remote"

  val catalog1 = Catalog("Catalog1", tmpRemoteRoot)
  val database1 = catalog1.database("Database1")

  val mockedProtocol1 = mock(classOf[NodeProtocol])
  when(mockedProtocol1.availableShards(database1.catalog.name, database1.name))
    .thenReturn(Set((1L, 0), (1L, 1)))

  val mockedProtocol2 = mock(classOf[NodeProtocol])
  when(mockedProtocol2.availableShards(database1.catalog.name, database1.name))
    .thenReturn(Set((1L, 0), (2L, 0)))

  val chomp = new Chomp {
    override val databases = Seq(database1)
    override val localNode = Node("Node1")
    override val nodes = Map(
      Node("Node1") -> Endpoint("Endpoint1"),
      Node("Node2") -> Endpoint("Endpoint2")
    )
    override def nodeProtocol = Map(
      Node("Node1") -> mockedProtocol1,
      Node("Node2") -> mockedProtocol2
    )
    override val nodeAlive = mock(classOf[NodeAlive])
    when(nodeAlive.isAlive(Node("Node1"))).thenReturn(true)
    when(nodeAlive.isAlive(Node("Node2"))).thenReturn(false)
    override val replicationFactor = 1
    override val replicationBeforeVersionUpgrade = 1
    override val maxDownloadRetries = 3
    override val executor = mock(classOf[ScheduledExecutorService])
    override val nodesAliveFreq = 1.minute
    override val nodesContentFreq = 1.minute 
    override val servingVersionsFreq = 1.minute   
    override val rootDir = tmpLocalRoot

    override def serializeMapReduce[T, U](mapReduce: MapReduce[T, U]) = "identity"
    
    override def deserializeMapReduce(mapReduce: String): MapReduce[ByteBuffer, _] = mapReduce match {
      case "identity" => new MapReduce[ByteBuffer, Seq[ByteBuffer]] {
        def map(t: ByteBuffer) = Seq(t)
        def reduce(t1: Seq[ByteBuffer], t2: Seq[ByteBuffer]): Seq[ByteBuffer] = t1 ++ t2
      }
    }

    @throws(classOf[IOException])
    override def serializeMapReduceResult(result: Any): Array[Byte] = {
      val b = new ByteArrayOutputStream()
      val o = new ObjectOutputStream(b)
      o.writeObject(result)
      o.flush()
      o.close()
      b.toByteArray()
    }

    @throws(classOf[IOException])
    override def deserializeMapReduceResult[T: TypeTag](result: Array[Byte]): T = {
      val b = new ByteArrayInputStream(result)
      val o = new ObjectInputStream(b)
      o.readObject().asInstanceOf[T]
    }
  }

  "Chomp" should {
    /* main method */

  "serialize a MapReduce function to a string" in {
    val mr = new MapReduce[ByteBuffer, Seq[ByteBuffer]] {
      def map(t: ByteBuffer) = Seq(t)
      def reduce(t1: Seq[ByteBuffer], t2: Seq[ByteBuffer]): Seq[ByteBuffer] = t1 ++ t2
    }

    chomp.serializeMapReduce[ByteBuffer, Seq[ByteBuffer]](mr) should be === "identity"
  }

  "deserialize a string to a MapReduce function" in {
    val deserializedMR = chomp.deserializeMapReduce("identity")

    val mr = new MapReduce[ByteBuffer, Seq[ByteBuffer]] {
      def map(t: ByteBuffer) = Seq(t)
      def reduce(t1: Seq[ByteBuffer], t2: Seq[ByteBuffer]): Seq[ByteBuffer] = t1 ++ t2
    }

    deserializedMR.isInstanceOf[MapReduce[ByteBuffer, Seq[ByteBuffer]]] should be === true
  }

  "serialize and deserialize a MapReduce result to and from a byte array" in {
    val result = Seq(1, 2, 3)
    val serializedResult = chomp.serializeMapReduceResult(result)
    val deserializedResult = chomp.deserializeMapReduceResult[Seq[Int]](serializedResult)

    deserializedResult should be === result
  } 

    "given a database, reference a local version of that database" in {
      val database1Local = chomp.localDB(database1)

      database1Local.catalog.name should be === database1.catalog.name
      database1Local.name should be === database1.name
      database1Local.catalog.base should be === chomp.rootDir
    }

    "initialize the set of available shards, if there are no shards available" in {
      chomp.availableShards should be === Set.empty[DatabaseVersionShard]
      chomp.initializeAvailableShards()
      chomp.availableShards should be === Set.empty[DatabaseVersionShard]
    }

    "initialize the set of available shards" in {
      val database1Local = chomp.localDB(database1)

      database1Local.versionedStore.createVersion(1L)

      createEmptyShard(database1Local.versionedStore, 1L)
      createEmptyShard(database1Local.versionedStore, 1L)

      database1Local.versionedStore.succeedVersion(1L, 2)

      chomp.initializeAvailableShards()

      chomp.availableShards should be === Set(
        DatabaseVersionShard(database1.catalog.name, database1.name, 1L, 0),
        DatabaseVersionShard(database1.catalog.name, database1.name, 1L, 1)
      )
    }

    "purge inconsistent shards within the Chomp's filesystem" in {
      val path = chomp.localDB(database1).versionedStore.versionPath(1L)
      val blobFilePath = path / "2.blob"
      val indexFilePath = path / "4.index"

      blobFilePath.touch()
      indexFilePath.touch()

      blobFilePath.exists should be === true
      indexFilePath.exists should be === true

      chomp.purgeInconsistentShards()

      blobFilePath.exists should be === false
      indexFilePath.exists should be === false
      (path / "0.blob").exists should be === true
    }

    "add a version to the map of local databases to versions being served" in {
      chomp.servingVersions should be === Map.empty[Database, Option[Long]]
      chomp.initializeServingVersions()
      chomp.servingVersions should be === Map(database1 -> Some(1L))
    }

    /* other methods */
    "download appropriate shards for given database version" in {
      val database1Local = chomp.localDB(database1)

      database1Local.versionedStore.versionExists(2L) should be === false

      database1.versionedStore.createVersion(2L)
      createEmptyShard(database1.versionedStore, 2L)
      createEmptyShard(database1.versionedStore, 2L)
      database1.versionedStore.succeedVersion(2L, 2)

      chomp.downloadDatabaseVersion(database1, 2L)

      database1Local.versionedStore.versionExists(2L) should be === true
      (database1Local.versionedStore.versionPath(2L) / "0.blob").exists should be === true
      (database1Local.versionedStore.versionPath(2L) / "0.index").exists should be === true
      database1Local.versionedStore.shardMarker(2L, 0).exists should be === true
      (database1Local.versionedStore.versionPath(2L) / "1.blob").exists should be === false
      (database1Local.versionedStore.versionPath(2L) / "1.index").exists should be === false
      database1Local.versionedStore.shardMarker(2L, 1).exists should be === false

      chomp.availableShards.contains(DatabaseVersionShard(database1.catalog.name, database1.name, 2L, 0)) should be === true
      chomp.availableShards.contains(DatabaseVersionShard(database1.catalog.name, database1.name, 2L, 1)) should be === false
    }

    "begin serving a given database version" in {
      chomp.servingVersions should be === Map(database1 -> Some(1L))
      chomp.serveVersion(database1, Some(2L))
      chomp.servingVersions.getOrElse(database1, None) should be === Some(2L)
    }    

    "determine the latest database version to download, if any" in {
      database1.versionedStore.createVersion(3L)
      createEmptyShard(database1.versionedStore, 3L)
      createEmptyShard(database1.versionedStore, 3L)
      createEmptyShard(database1.versionedStore, 3L)
      database1.versionedStore.succeedVersion(3L, 3)

      chomp.getNewVersionNumber(database1) should be === Some(3L)
    }

    "update a database to the latest version" in {
      chomp.servingVersions.getOrElse(database1, None) should be === Some(2L)
      chomp.localDB(database1).versionedStore.versionExists(3L) should be === false

      chomp.updateDatabase(database1)

      chomp.localDB(database1).versionedStore.versionExists(3L) should be === true

      chomp.availableShards.contains(DatabaseVersionShard(database1.catalog.name, database1.name, 3L, 0)) should be === true
      chomp.availableShards.contains(DatabaseVersionShard(database1.catalog.name, database1.name, 3L, 1)) should be === false
      chomp.availableShards.contains(DatabaseVersionShard(database1.catalog.name, database1.name, 3L, 2)) should be === true
    }

    "initialize numShardsPerVersion" in {
      chomp.numShardsPerVersion should be === Map.empty[(Database, Long), Int]
      chomp.initializeNumShardsPerVersion()
      chomp.numShardsPerVersion should be === Map(
        (database1, 1L) -> 2,
        (database1, 2L) -> 2,
        (database1, 3L) -> 3
      )
    }

    "update the set of nodes alive" in {
      chomp.nodesAlive should be === Map.empty[Node, Boolean]
      chomp.updateNodesAlive()
      chomp.nodesAlive should be === Map(Node("Node1") -> true, Node("Node2") -> false)
    }

    "update the map of remote nodes to DatabaseVersionShards available" in {
      chomp.nodesContent should be === Map.empty[Node, Set[DatabaseVersionShard]]
      chomp.updateNodesContent()
      chomp.nodesContent should be === Map(
        Node("Node1") -> Set(
          DatabaseVersionShard(database1.catalog.name, database1.name, 1L, 0),
          DatabaseVersionShard(database1.catalog.name, database1.name, 1L, 1)
        ),
        Node("Node2") -> Set(
          DatabaseVersionShard(database1.catalog.name, database1.name, 1L, 0),
          DatabaseVersionShard(database1.catalog.name, database1.name, 2L, 0)
        )
      )
    }

  }
}