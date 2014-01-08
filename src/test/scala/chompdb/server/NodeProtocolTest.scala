package chompdb.server

import chompdb._
import chompdb.server._

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

  val cat1 = new Catalog("Catalog1", tmpLocalRoot.fs, tmpLocalRoot.root)
  val db1 = cat1.database("Database1")

  val node1 = Node("Node1")
  val node2 = Node("Node2")
  val allNodes = Set(node1, node2)

  lazy val nodeProtocols = {
    chomps 
      .map { case (node, chomp) => node -> new Chomp.LocalNodeProtocol(node, chomp) } 
      .toMap
  }

  val chomps: Set[(Node, Chomp)] = allNodes
    .zipWithIndex
    .map { nodeAndIndex =>
      nodeAndIndex._1 -> new Chomp {
        override val databases = Seq(db1)
        override val nodes = allNodes map { n => (n, Endpoint("Endpoint" + n.id takeRight 1)) } toMap
        override val nodeAlive = mock(classOf[NodeAlive])
        override val replicationFactor = 1
        override val replicationBeforeVersionUpgrade = 1
        override val shardIndex = 0
        override val totalShards = 1
        override val executor = mock(classOf[ScheduledExecutorService])
        override val fs = tmpLocalRoot.fs
        override val rootDir = tmpLocalRoot.root /+ ("Chomp" + nodeAndIndex._2.toString)

        override def nodeProtocol = nodeProtocols
    }
  }

}