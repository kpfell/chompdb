package chompdb.store

import chompdb.sharding._
import f1lesystem.FileSystem
import java.io._
import scala.collection._
import java.util.concurrent.atomic.AtomicInteger

object SpillingStore {
  
}

trait SpillingStore extends Store.Writer {
  val baseDir: FileSystem#Dir

  val shards: Sharded

  private[chompdb] lazy val nextId = new IdGenerator(shards)

  val spills = new AtomicInteger()
  
  def newWriter = {
    val number = spills.incrementAndGet()
    new FileStore.Writer {
      val baseFile: f1lesystem.FileSystem#File = ???      
      val shards: chompdb.sharding.Sharded = ??? 
    }
  }
  def put(value: Array[Byte]): Long
  
}