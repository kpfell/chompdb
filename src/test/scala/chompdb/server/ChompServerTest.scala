package chompdb.server

import chompdb._
import f1lesystem.LocalFileSystem
import java.util.concurrent.ScheduledExecutorService

import org.mockito.Mockito.{ mock, when }
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChompServerTest extends WordSpec with ShouldMatchers {
  val testName = "ChompServerTest"

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

  val testChomp = new Chomp {
    val databases = Seq(mock(classOf[Database]))
    val nodes = Map(Node("Node1") -> Endpoint("endpoint1"), 
      Node("Node2") -> Endpoint("endpoint2"))
    val nodeProtocolInfo = new NodeProtocolInfo {
      def availableShards(n: Node): Set[DatabaseVersionShard] = {
        Set(
          DatabaseVersionShard("TbiCatalog", "TbiDatabase1", 1L, 1),
          DatabaseVersionShard("TbiCatalog", "TbiDatabase1", 1L, 2),
          DatabaseVersionShard("TbiCatalog", "TbiDatabase2", 2L, 1)
        )
      }
    }
    val nodeAlive = mock(classOf[NodeAlive])
    when(nodeAlive.isAlive(Node("Node1"))).thenReturn(true)
    when(nodeAlive.isAlive(Node("Node2"))).thenReturn(false)
    val replicationFactor = 1
    val replicationFactorBeforeVersionUpgrade = 1
    val shardIndex = 0
    val totalShards = 1
    val executor = mock(classOf[ScheduledExecutorService])
    val fs = tmpLocalRoot.fs
    val rootDir = tmpLocalRoot.root
  }

  "ChompServer" should {
    val chompServer = new ChompServer {
      val chomp = testChomp
    }

    "update the internal map of nodes alive" in {
      chompServer.nodesAlive should be === Map()

      chompServer.updateNodesAlive()

      chompServer.nodesAlive should be === Map(
        Node("Node1") -> true, 
        Node("Node2") -> false
      )
    }

    "update the internal map of nodes' content" in {
      chompServer.nodesContent should be === Map()

      chompServer.updateNodesContent()

      chompServer.nodesContent should be === Map(
        Node("Node1") -> Set(
          DatabaseVersionShard("TbiCatalog", "TbiDatabase1", 1L, 1),
          DatabaseVersionShard("TbiCatalog", "TbiDatabase1", 1L, 2),
          DatabaseVersionShard("TbiCatalog", "TbiDatabase2", 2L, 1)
        ),
        Node("Node2") -> Set(
          DatabaseVersionShard("TbiCatalog", "TbiDatabase1", 1L, 1),
          DatabaseVersionShard("TbiCatalog", "TbiDatabase1", 1L, 2),
          DatabaseVersionShard("TbiCatalog", "TbiDatabase2", 2L, 1)
        )
      )
    }
  }
}