package chompdb.server

import chompdb._
import chompdb.Catalog
import chompdb.Database
import f1lesystem.LocalFileSystem
import java.util.concurrent.ScheduledExecutorService

import org.mockito.Mockito.{ mock, verify, when }
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class NodeProtocolTest extends WordSpec with ShouldMatchers {
  val testName = "NodeProtocolTest" 

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

  val catLocal = Catalog("TestCatalog1", tmpLocalRoot.fs, tmpLocalRoot.root)
  val catRemote = Catalog("TestCatalog2", tmpRemoteRoot.fs, tmpRemoteRoot.root)
  val db1 = catLocal.database("TestDatabase1")
  val db2 = catRemote.database("TestDatabase2")

  val npi = new NodeProtocolInfo {
    def allAvailableShards(n: Node) = Set[DatabaseVersionShard]()
    def availableShards(n: Node, db: Database) = Set[DatabaseVersionShard]()
    def availableShardsForVersion(n: Node, db: Database, v: Long) = n match {
      case Node("Node1") => 
        Set(
          DatabaseVersionShard(db.catalog.name, db.name, v, 1),
          DatabaseVersionShard(db.catalog.name, db.name, v, 2)
        )

      case Node("Node2") =>
        Set(DatabaseVersionShard(db.catalog.name, db.name, v, 1))
    }
    
    def availableVersions(n: Node, db: Database) = Set()

    def latestVersion(n: Node, db: Database) = n match {
      case Node("Node1") => None
      case Node("Node2") => Some(2L)
    }

    def serveVersion(n: Node, db: Database, v: Option[Long]) = true
    def retrieveVersionsServed(n: Node) = Map()
  }

  val testChomp = new Chomp {
    val databases = Seq(db1)
    val nodes = Map(
      Node("Node1") -> Endpoint("Endpoint1"), 
      Node("Node2") -> Endpoint("Endpoint2")
    )
    val nodeProtocolInfo = npi
    val nodeAlive = mock(classOf[NodeAlive])
    val replicationFactor = 2
    val replicationBeforeVersionUpgrade = 2
    val shardIndex = 0
    val totalShards = 1
    val executor = mock(classOf[ScheduledExecutorService])
    val fs = tmpLocalRoot.fs
    val rootDir = tmpLocalRoot.root
  }
  
  val testUnitChomp = new Chomp {
    val databases = Seq(db1)
    val nodes = Map(
      Node("Node1") -> Endpoint("Endpoint1"),
      Node("Node2") -> Endpoint("Endpoint2"),
      Node("Node3") -> Endpoint("Endpoint3")
    )
    val nodeProtocolInfo = mock(classOf[NodeProtocolInfo])
    val nodeAlive = mock(classOf[NodeAlive])
    val replicationFactor = 2
    val replicationBeforeVersionUpgrade = 2
    val shardIndex = 0
    val totalShards = 1
    val executor = mock(classOf[ScheduledExecutorService])
    val fs = tmpLocalRoot.fs
    val rootDir = tmpLocalRoot.root
  }

  "NodeProtocol client-side" should {
    "message every other node in the network to serve a new version" in {
      testUnitChomp
        .nodeProtocol
        .remoteNodesServeVersion(db1, Some(1L))

      verify(testUnitChomp.nodeProtocolInfo)
        .serveVersion(Node("Node1"), db1, Some(1L))

      verify(testUnitChomp.nodeProtocolInfo)
        .serveVersion(Node("Node2"), db1, Some(1L))

      verify(testUnitChomp.nodeProtocolInfo)
        .serveVersion(Node("Node3"), db1, Some(1L))
    }

    "return empty set when no remote shards exist for a given database" in {
      testChomp
        .nodeProtocol
        .allAvailableShards(mock(classOf[Node])) should be === Set.empty
    }

    "return a map of Nodes to DatabaseVersionShards available for a given database and version" in {
      testChomp
        .nodeProtocol
        .versionShardsPerNode(db1, 1L) should be === Map(
          Node("Node1") -> Set(
            DatabaseVersionShard(db1.catalog.name, db1.name, 1L, 1),
            DatabaseVersionShard(db1.catalog.name, db1.name, 1L, 2)
          ),
          Node("Node2") -> Set(DatabaseVersionShard(db1.catalog.name, db1.name, 1L, 1))
        )
    }
  }

  "NodeProtocol server-side" should {
    "return empty set when no shards exist for any local database" in {
      testChomp
        .nodeProtocol
        .allLocalShards should be === Set.empty
    }

    "return set of all DatabaseVersionShards available locally" in {      
      db1.createVersion(1L)
      db1.createEmptyShard(1L)
      db1.createEmptyShard(1L)
      db1.succeedVersion(1L, 2)

      testChomp
        .nodeProtocol
        .allLocalShards
        .size should be === 2
    }

    "return set of DatabaseVersionShards available locally for a given Database" in {
      testChomp.nodeProtocol.localShards(db1) should be === Set(
        DatabaseVersionShard(db1.catalog.name, db1.name, 1L, 0),
        DatabaseVersionShard(db1.catalog.name, db1.name, 1L, 1)
      )
    }

    "update the Database version being served locally, when the version has already been downloaded" in {
      testChomp.servingVersions should be === Map.empty

      db1.createVersion(3L)
      db1.succeedVersion(3L, 0)

      testChomp.nodeProtocol.serveVersion(db1, Some(3L))

      testChomp.servingVersions should be === Map(db1 -> Some(3L))
    }
  }
}