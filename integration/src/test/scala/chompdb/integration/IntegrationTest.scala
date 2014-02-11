package chompdb.integration

import chompdb._
import chompdb.server._
import f1lesystem._
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.Executors
import scala.concurrent.duration._
import chompdb.server.SlapChop

object IntegrationTest extends App {
  import Utils._

  val tmp = LocalFileSystem.parseDirectory(System.getProperty("java.io.tmpdir"))

  val catalog = Catalog("catalog1", tmp /+ "catalog")

  val params = Params(
    Seq(new Database(catalog, "database1")-> DatabaseParams(
      nElements = 100,
      shardsTotal = 10,
      blobSizeRange = (10 * 1024, 100 * 1024)
    ))
  )

  val creator = new DatabaseCreator {
    override val params = IntegrationTest.params
    override val localDir = tmp /+ "local"
    override val delayBetweenVersions = 30.seconds
    override val numThreads = 2
  }

  val nServers = 3

  val nodes: Map[Node, Endpoint] = (1 to nServers) map { n =>
    Node(n.toString) -> Endpoint(n.toString)
  } toMap;

  val servers: Map[Node, DatabaseServer] = nodes map { case (node, endpoint) => node -> server(node.id.toInt, node) }

  def server(index: Int, node: Node) = new DatabaseServer {
    override val params = IntegrationTest.params

    override val databases: Seq[Database] = IntegrationTest.params.databases map (_._1)
    override val localNode: Node = node
    override val nodes: Map[Node, Endpoint] = IntegrationTest.nodes
    override val nodeAlive: NodeAlive = new NodeAlive {
      override def isAlive(node: Node) = true
      override def imAlive() = () // no-op
    }
    override val replicationFactor: Int = 2
    override val replicationBeforeVersionUpgrade: Int = 2
    override val maxDownloadRetries: Int = 10
    override val executor: ScheduledExecutorService = Executors.newScheduledThreadPool(5)
    override val rootDir: FileSystem#Dir = {
      val dir = tmp /+ "server" /+ index.toString
      dir.mkdir()
      dir
    }

    override val nodeProtocol: Map[Node, NodeProtocol] = nodes map { case (node, endpoint) => node -> new NodeProtocol {
      override def availableShards(catalog: String, database: String): Set[VersionShard] = {
        val available = servers(node).availableShards
        available collect {
          case a if (a.catalog == catalog && a.database == database) => (a.version, a.shard)
        } toSet
      }

      override def mapReduce(catalog: String, database: String, version: Long, ids: Seq[Long], mapReduce: String): Array[Byte] = {
        val server = servers(node)
        val mapReduceF = server.deserializeMapReduce(mapReduce)
        val result = server.mapReduce(catalog, database, ids, mapReduceF)
        server.serializeMapReduceResult(result)
      }
    }} toMap;

    override def toString = s"Server($index, $node)"

  }

  val creatorThread = thread("creator") { creator.run() }

  val serverThreads = servers map { case (node, server) =>
    thread(s"server-${node.id}") {
      server.run()
    }
  }

  val client = new DatabaseClient {
    override val delayBetweenQueries = 5.seconds
    override val blocksPerQueryRange: (Int, Int) = (1, 10)
    override val servers: Seq[SlapChop] = IntegrationTest.servers.values.toSeq
    override val numClients = 1
    override val params = IntegrationTest.params
    override val scheduledExecutor: ScheduledExecutorService = Executors.newScheduledThreadPool(5)
  }
  client.run()

  Thread.sleep(60.seconds.toMillis)

  println("End of testing; shutting down ...")
  creator.stop()
  client.stop()

  creatorThread.join()
  println("End.")
}

object Utils {
  def thread(name: String)(f: => Unit) = {
    val t = new Thread() {
      override def run() = { f }
    }
    t.start()
    t
  }
}