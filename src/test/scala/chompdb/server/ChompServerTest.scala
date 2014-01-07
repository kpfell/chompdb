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

  val cat = new Catalog("TestCatalog", tmpLocalRoot.fs, tmpLocalRoot.root)
  val db1 = cat.database("TestDatabase1")
  val db2 = cat.database("TestDatabase2")

  val testChomp = new Chomp {
    // val databases = Seq(mock(classOf[Database])) // Need to reimplement nodeProtocolInfo if this is implemented
    val databases = Seq(db1)
    val nodes = Map(Node("Node1") -> Endpoint("endpoint1"), 
      Node("Node2") -> Endpoint("endpoint2"))
    val nodeProtocolInfo = new NodeProtocolInfo {
      def availableShards(n: Node, db: Database): Set[DatabaseVersionShard] = {
        Set(
          DatabaseVersionShard(db.catalog.name, db.name, 1L, 1),
          DatabaseVersionShard(db.catalog.name, db.name, 1L, 2),
          DatabaseVersionShard(db2.catalog.name, db2.name, 2L, 1)
        )
      }

      def availableShardsForVersion(n: Node, db: Database, v: Long): Set[DatabaseVersionShard] = {
        Set(
          DatabaseVersionShard(db.catalog.name, db.name, v, 1),
          DatabaseVersionShard(db.catalog.name, db.name, v, 2)
        )
      }

      def availableVersions(n: Node, db: Database) = Set()

      def latestVersion(n: Node, db: Database): Option[Long] = {
        if (db.name == "TbiDatabase1") Some(1L)
        else if (db.name == "TbiDatabase2") Some(2L)
        else None
      }

      def serveVersion(n: Node, db: Database, v: Option[Long]): Boolean = true

      def retrieveVersionsServed(n: Node): Map[Database, Option[Long]] = Map()
    }
    val nodeAlive = mock(classOf[NodeAlive])
    when(nodeAlive.isAlive(Node("Node1"))).thenReturn(true)
    when(nodeAlive.isAlive(Node("Node2"))).thenReturn(false)
    val replicationFactor = 1
    val replicationBeforeVersionUpgrade = 1
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
          DatabaseVersionShard(db1.catalog.name, db1.name, 1L, 1),
          DatabaseVersionShard(db1.catalog.name, db1.name, 1L, 2),
          DatabaseVersionShard(db2.catalog.name, db2.name, 2L, 1)
        ),
        Node("Node2") -> Set(
          DatabaseVersionShard(db1.catalog.name, db1.name, 1L, 1),
          DatabaseVersionShard(db1.catalog.name, db1.name, 1L, 2),
          DatabaseVersionShard(db2.catalog.name, db2.name, 2L, 1)
        )
      )
    }
  }
}