package chompdb.server

import chompdb._
import chompdb.Catalog
import chompdb.Database
import f1lesystem.LocalFileSystem
import java.util.concurrent.ScheduledExecutorService

import org.mockito.Mockito.{ mock, when }
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

  val cat = Catalog("TestCatalog", tmpLocalRoot.fs, tmpLocalRoot.root)
  val db1 = cat.database("TestDatabase1")
  val db2 = cat.database("TestDatabase2")

  val npi = new NodeProtocolInfo {
    def allAvailableShards(n: Node) = Set[DatabaseVersionShard]()
    def availableShards(n: Node, db: Database) = Set[DatabaseVersionShard]()
    def availableShardsForVersion(n: Node, db: Database, v: Long) = n match {
      case Node("Node1") => 
        Set(
          DatabaseVersionShard(cat.name, db.name, v, 1),
          DatabaseVersionShard(cat.name, db.name, v, 2)
        )

      case Node("Node2") =>
        Set(DatabaseVersionShard(cat.name, db.name, v, 1))
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

  "NodeProtocol client-side" should {
    "return the latest remote versions for a given database" in {
      testChomp
        .nodeProtocol
        .latestRemoteVersions(db1) should be === Set(None, Some(2L))
    }

    "return empty set when no remote shards exist for a given database" in {
      testChomp
        .nodeProtocol
        .allAvailableShards(mock(classOf[Node])) should be === Set.empty
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
        DatabaseVersionShard(cat.name, db1.name, 1L, 0),
        DatabaseVersionShard(cat.name, db1.name, 1L, 1)
      )
    }

    "update the Database version being served locally" in {
      testChomp.servingVersions should be === Map()

      testChomp.nodeProtocol.serveVersion(db1, Some(1L))

      testChomp.servingVersions should be === Map(db1 -> Some(1L))
    }

    """|set cluster to serve the latest local version, if that version meets 
       |minimum replicationFactor requirements and is the newest version
       |across the cluster""".stripMargin in {

    }
  }
}