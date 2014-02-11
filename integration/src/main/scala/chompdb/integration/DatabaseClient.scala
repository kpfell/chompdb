package chompdb.integration

import chompdb._
import chompdb.store._
import f1lesystem._
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicBoolean
import chompdb.server.MapReduce
import chompdb.server.SlapChop
import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.duration._
import java.nio.ByteBuffer

case class Result(
  versions: Set[Long],
  ids: Set[Long]
)

trait DatabaseClient {

  val delayBetweenQueries: Duration

  val blocksPerQueryRange: (Int, Int)

  val servers: Seq[SlapChop]

  val numClients: Int

  val params: Params

  val scheduledExecutor: ScheduledExecutorService

  private val random = new RandomUtils(); import random._

  private val stopFlag = new AtomicBoolean(false)

  def randomDelay = Duration(randomLong(0, delayBetweenQueries._1), delayBetweenQueries._2)

  val mapReduce = new MapReduce[ByteBuffer, Result] {
    override def map(buf: ByteBuffer) = {
      val data = Blob.unapply(buf.array)
      Result(Set(data.version), Set(data.writer))
    }

    override def reduce(r1: Result, r2: Result) = {
      Result(r1.versions ++ r2.versions, r1.ids ++ r2.ids)
    }
  }

  def run() {

    val clients = (1 to numClients) map { i =>
      new Runnable {
        override def run() {
          val (db, dbParams) = pick(params.databases)
          val randomServer = random.pick(servers)
          try {
	          println(s"Client $i querying server $randomServer database $db...")
	          val keys = {
	            val n = randomInt(blocksPerQueryRange._1, blocksPerQueryRange._2)
	            (1 to n) map { _ => randomLong(0, dbParams.nElements) }
	          }
	          val result = randomServer.mapReduce(db.catalog.name, db.name, keys, mapReduce)
	          println(s"mapReduce result: $result")
          } catch { case e: Throwable =>
            e.printStackTrace()
          }
          if (!stopFlag.get) {
            scheduledExecutor.schedule(this, delayBetweenQueries._1, delayBetweenQueries._2)
          }
        }
      }
    }

    clients foreach { client =>
      val r = randomDelay
      scheduledExecutor.schedule(client, r._1, r._2)
    }
  }

  def stop() {
    stopFlag.set(true)
  }
}
