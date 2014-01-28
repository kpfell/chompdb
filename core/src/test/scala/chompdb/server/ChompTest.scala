package chompdb.server

import chompdb._
import chompdb.store._
import chompdb.testing._
import f1lesystem.{ FileSystem, LocalFileSystem }
import java.util.concurrent.ScheduledExecutorService
import scala.collection._
import scala.collection.mutable.SynchronizedSet

import org.mockito.Mockito.{ mock, when }
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChompTest extends WordSpec with ShouldMatchers {
  import TestUtils.stringToByteArray
  import TestUtils.byteArrayToString
  import TestUtils.createEmptyShard

  val testName = "ChompTest"

  val tmpLocalRoot = new LocalFileSystem.TempRoot {
    override val rootName = "local"
    override lazy val root: fs.Dir = {
      val tmp = fs.parseDirectory(System.getProperty("java.io.tmpdir")) /+ testName /+ rootName
      if (tmp.exists) {
        tmp.deleteRecursively()
      }
      tmp.mkdir()
      tmp
    }
  }

  val tmpLocalRoot2 = new LocalFileSystem.TempRoot {
    override val rootName = "local"
    override lazy val root: fs.Dir = {
      val tmp = fs.parseDirectory(System.getProperty("java.io.tmpdir")) /+ "ChompGetterTest" /+ rootName
      if (tmp.exists) {
        tmp.deleteRecursively()
      }
      tmp.mkdir()
      tmp
    }
  }  

  val tmpRemoteRoot = new LocalFileSystem.TempRoot {
    override val rootName = "remote"
    override lazy val root: fs.Dir = {
      val tmp = fs.parseDirectory(System.getProperty("java.io.tmpdir")) /+ testName /+ rootName
      if (tmp.exists) {
        tmp.deleteRecursively()
      }
      tmp.mkdir()
      tmp
    }
  }

  val catalog1 = Catalog("Catalog1", tmpRemoteRoot.fs, tmpRemoteRoot.root)
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
    override val fs = tmpLocalRoot.fs
    override val rootDir = tmpLocalRoot.root

    override def serializeMapReduce[T, U](mapReduce: MapReduce[T, U]) = "identity"
  }

  "Chomp" should {
    /* main method */

    "given a database, reference a local version of that database" in {
      val database1Local = chomp.localDB(database1)

      database1Local.catalog.name should be === database1.catalog.name
      database1Local.name should be === database1.name
      database1Local.catalog.fs should be === chomp.fs
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

    "return a blob that is stored locally" in {
      val catalog2 = Catalog("catalog2", tmpLocalRoot2.fs, tmpLocalRoot2.root)
      val database3 = catalog2.database("database3")
      database3.versionedStore.createVersion(0L)

      val numThreads = 1

      val writers = (0 until numThreads) map { i =>
        new ShardedWriter {
          val writers = numThreads
          val writerIndex = i
          val shardsTotal = 1
          val baseDir: FileSystem#Dir = database3.versionedStore.versionPath(0L)
        }
      }
      
      val ids = new mutable.HashSet[Long] with SynchronizedSet[Long]
      
      val threads = (1 to numThreads) map { n =>
        new Thread("ChompGetterTest") {
          override def run() {
            (1 to 100) foreach { x =>
              val id = writers(n-1).put(s"This is a test: thread $n element $x")
              ids += id
            }            
          }
        }
      }
      
      threads foreach { _.start() }
      threads foreach { _.join() }
      
      writers foreach { _.close() }

      ids should be === Set((0 until 100): _*)

      database3.versionedStore.succeedVersion(0L, 1)

      val mockedProtocol3 = mock(classOf[NodeProtocol])
      when(mockedProtocol3.availableShards(database3.catalog.name, database3.name))
        .thenReturn(Set((0L, 0)))

      val getterChomp: Chomp = new Chomp {
        override val databases = Seq(database3)
        override val localNode = Node("Node1")
        override val nodes = Map(Node("Node1") -> Endpoint("Endpoint1"))
        override val nodeAlive = mock(classOf[NodeAlive])
        override val replicationFactor = 1
        override val replicationBeforeVersionUpgrade = 1
        override val maxDownloadRetries = 3
        override val executor = mock(classOf[ScheduledExecutorService])
        override val fs = tmpLocalRoot2.fs
        override val rootDir = tmpLocalRoot2.root

        override def nodeProtocol = Map(Node("Node1") -> mockedProtocol3)

        override def serializeMapReduce[T, U](mapReduce: MapReduce[T, U]) = "identity"
      }

      getterChomp
        .nodeProtocol(Node("Node1"))
        .availableShards(database3.catalog.name, database3.name) should be === Set((0L, 0))

      getterChomp.initializeAvailableShards()
      getterChomp.initializeServingVersions()
      getterChomp.initializeNumShardsPerVersion()
      getterChomp.updateNodesContent()

      val value = getterChomp.getBlob(database3.catalog.name, database3.name, 0L) 
      new String(value.array()) should be === "This is a test: thread 1 element 1"
    }

    "return a blob that is stored on another node" in {
      val catalog3 = Catalog("catalog3", tmpLocalRoot2.fs, tmpLocalRoot2.root)
      val database4 = catalog3.database("database4")
      database4.versionedStore.createVersion(0L)

      val numThreads = 1

      val writers = (0 until numThreads) map { i =>
        new ShardedWriter {
          val writers = numThreads
          val writerIndex = i
          val shardsTotal = 1
          val baseDir: FileSystem#Dir = database4.versionedStore.versionPath(0L)
        }
      }
      
      val ids = new mutable.HashSet[Long] with SynchronizedSet[Long]
      
      val threads = (1 to numThreads) map { n =>
        new Thread("ChompGetterTest") {
          override def run() {
            (1 to 100) foreach { x =>
              val id = writers(n-1).put(s"This is a test: thread $n element $x")
              ids += id
            }            
          }
        }
      }
      
      threads foreach { _.start() }
      threads foreach { _.join() }
      
      writers foreach { _.close() }

      ids should be === Set((0 until 100): _*)

      database4.versionedStore.succeedVersion(0L, 1)

      val mockedProtocol3 = mock(classOf[NodeProtocol])
      when(mockedProtocol3.availableShards(database4.catalog.name, database4.name))
        .thenReturn(Set.empty[(Long, Int)])

      val mockedProtocol4 = mock(classOf[NodeProtocol])
      when(mockedProtocol4.availableShards(database4.catalog.name, database4.name))
        .thenReturn(Set((0L, 0)))

      val getterChomp1 = new Chomp {
        override val databases = Seq(database4)
        override val localNode = Node("Node1")
        override val nodes = Map(
          Node("Node1") -> Endpoint("Endpoint1"),
          Node("Node2") -> Endpoint("Endpoint2")
        )  
        override val nodeAlive = mock(classOf[NodeAlive])
        override val replicationFactor = 1
        override val replicationBeforeVersionUpgrade = 1
        override val maxDownloadRetries = 3
        override val executor = mock(classOf[ScheduledExecutorService])
        override val fs = tmpLocalRoot2.fs
        override val rootDir = tmpLocalRoot2.root

        override def nodeProtocol = Map(
          Node("Node1") -> mockedProtocol3,
          Node("Node2") -> mockedProtocol4
        )

        override def serializeMapReduce[T, U](mapReduce: MapReduce[T, U]) = "identity"
      }

      val getterChomp2 = new Chomp {
        override val databases = Seq(database4)
        override val localNode = Node("Node2")
        override val nodes = Map(
          Node("Node1") -> Endpoint("Endpoint1"),
          Node("Node2") -> Endpoint("Endpoint2")
        )
        override val nodeAlive = mock(classOf[NodeAlive])
        override val replicationFactor = 1
        override val replicationBeforeVersionUpgrade = 1
        override val maxDownloadRetries = 3
        override val executor = mock(classOf[ScheduledExecutorService])
        override val fs = tmpLocalRoot2.fs
        override val rootDir = tmpLocalRoot2.root

        override def nodeProtocol = Map(
          Node("Node1") -> mockedProtocol3,
          Node("Node2") -> mockedProtocol4
        )

        override def serializeMapReduce[T, U](mapReduce: MapReduce[T, U]) = "identity"
      }

      getterChomp1.initializeAvailableShards()
      getterChomp1.initializeServingVersions()
      getterChomp1.initializeNumShardsPerVersion()
      getterChomp1.updateNodesContent()

      getterChomp2.initializeAvailableShards()
      getterChomp2.initializeServingVersions()
      getterChomp2.initializeNumShardsPerVersion()
      getterChomp2.updateNodesContent()

      getterChomp1
        .nodeProtocol(Node("Node1"))
        .availableShards(database4.catalog.name, database4.name) should be === Set.empty[(Long, Int)]

      getterChomp1
        .nodeProtocol(Node("Node2"))
        .availableShards(database4.catalog.name, database4.name) should be === Set((0L, 0))

      getterChomp2
        .nodeProtocol(Node("Node1"))
        .availableShards(database4.catalog.name, database4.name) should be === Set.empty[(Long, Int)]

      getterChomp2
        .nodeProtocol(Node("Node2"))
        .availableShards(database4.catalog.name, database4.name) should be === Set((0L, 0))

      val value = getterChomp2.getBlob(database4.catalog.name, database4.name, 0L) 
      new String(value.array()) should be === "This is a test: thread 1 element 1"
    }
  }

}