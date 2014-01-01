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
  val db = cat.database("TestDatabase")

  val npi = new NodeProtocolInfo {
    def allAvailableShards(n: Node) = Set[DatabaseVersionShard]()
    def availableShards(n: Node, db: Database) = Set[DatabaseVersionShard]()
    def latestVersion(n: Node, db: Database) = None
    def serveVersion(n: Node, db: Database, v: Long) = true
  }

  val testChomp = new Chomp {
    val databases = Seq(db)
    val nodes = Map(Node("Node1") -> Endpoint("Endpoint1"))
    val nodeProtocolInfo = npi
    val nodeAlive = mock(classOf[NodeAlive])
    val replicationFactor = 1
    val replicationBeforeVersionUpgrade = 1
    val shardIndex = 0
    val totalShards = 1
    val executor = mock(classOf[ScheduledExecutorService])
    val fs = tmpLocalRoot.fs
    val rootDir = tmpLocalRoot.root
  }

  "NodeProtocol" should {
    "return empty set when no shards exist in any database" in {
      // CLIENT-SIDE
      testChomp
        .nodeProtocol
        .allAvailableShards(mock(classOf[Node])) should be === Set.empty

      // SERVER-SIDE
      testChomp
        .nodeProtocol
        .allLocalShards should be === Set.empty
    }

    "return set of all DatabaseVersionShards available locally" in {      
      // SERVER-SIDE
      db.createVersion(1L)
      db.createEmptyShard(1L)
      db.createEmptyShard(1L)
      db.succeedVersion(1L, 2)

      testChomp
        .nodeProtocol
        .allLocalShards
        .size should be === 2
    }

    "return set of DatabaseVersionShards available locally for a given Database" in {
      testChomp.nodeProtocol.localShards(db) should be === Set(
        DatabaseVersionShard(cat.name, db.name, 1L, 0),
        DatabaseVersionShard(cat.name, db.name, 1L, 1)
      )
    }
  }
}