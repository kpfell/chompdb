package chompdb.server

import chompdb._
import chompdb.server._
import chompdb.testing.TestUtils.createEmptyShard

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
  
  db1.versionedStore.createVersion(1L)
  createEmptyShard(db1.versionedStore, 1L)
  createEmptyShard(db1.versionedStore, 1L)
  createEmptyShard(db1.versionedStore, 1L)
  db1.versionedStore.succeedVersion(1L, 3)

  val node1 = Node("Node1")
  val node2 = Node("Node2")
  val allNodes = Set(node1, node2)

  lazy val nodeProtocols = {
    chomps 
      .map { case (node, chomp) => node -> new Chomp.LocalNodeProtocol(node, chomp) } 
      .toMap
  }

  val chomps: Set[(Node, Chomp)] = allNodes
    .map { node =>
      node -> new Chomp {
        override val databases = Seq(db1)
        override val nodes = allNodes map { n => (n, Endpoint("Endpoint" + n.id takeRight 1)) } toMap
        override val nodeAlive = mock(classOf[NodeAlive])
        override val replicationFactor = 1
        override val replicationBeforeVersionUpgrade = 1
        override val shardIndex = 0
        override val maxDownloadRetries = 3
        override val executor = mock(classOf[ScheduledExecutorService])
        override val fs = tmpLocalRoot.fs
        override val rootDir = tmpLocalRoot.root

        override def nodeProtocol = nodeProtocols
    }
  }

  chomps foreach { nodeAndChomp => nodeAndChomp._2.initializeAvailableShards() }

  "LocalNodeProtocol" should {
    "return the set of VersionShards available for a node, given a catalog name and database name" in {
      chomps foreach { clientNodeAndChomp => 
        chomps foreach { serverNodeAndChomp => 
          val nodeProtocol = clientNodeAndChomp._2.nodeProtocol(serverNodeAndChomp._1)
          nodeProtocol.availableShards(db1.catalog.name, db1.name) should be === Set((1L, 0), (1L, 1), (1L, 2))
        }
      }
    }

  }

}