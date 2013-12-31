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
    def availableShards(n: Node) = Set[DatabaseVersionShard]()
  }

  val testChomp = new Chomp {
    val databases = Seq(db)
    val nodes = mock(classOf[Map[Node, Endpoint]])
    val nodeProtocolInfo = npi
    val nodeAlive = mock(classOf[NodeAlive])
    val replicationFactor = 1
    val replicationFactorBeforeVersionUpgrade = 1
    val shardIndex = 0
    val totalShards = 1
    val executor = mock(classOf[ScheduledExecutorService])
    val fs = tmpLocalRoot.fs
    val rootDir = tmpLocalRoot.root
  }

  "NodeProtocol" should {
    "return empty set when no shards exist" in {
      // CLIENT-SIDE
      testChomp
        .nodeProtocol
        .availableShards(mock(classOf[Node])) should be === Set.empty

      // SERVER-SIDE
      testChomp
        .nodeProtocol
        .localShards should be === Set.empty
    }

    "return set of DatabaseVersionShards available locally" in {      
      // SERVER-SIDE
      db.createVersion(1L)
      db.createEmptyShard(1L)
      db.createEmptyShard(1L)
      db.succeedVersion(1L, 2)

      testChomp
        .nodeProtocol
        .localShards
        .size should be === 2
    }
  }
}