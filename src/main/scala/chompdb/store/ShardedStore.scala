package chompdb.store

import chompdb.sharding._
import f1lesystem.FileSystem
import java.io._
import scala.collection._
import java.util.concurrent.atomic.AtomicInteger

/** Non-thread-safe class; meant to be used by a single thread/writer. */
trait ShardedStore extends Store.Writer {
  val baseDir: FileSystem#Dir

  val shardsTotal: Int

  val writers: Int

  val writerIndex: Int

  private[chompdb] lazy val ownedShards = (0 until shardsTotal) filter (_ % writers == writerIndex)

  private[chompdb] lazy val shardWriters = ownedShards map { shardId =>
    new FileStore.Writer {
      override val baseFile = baseDir / shardId.toString
      override val shards = new Sharded {
        val shardsIndex = shardId
        val shardsTotal = ShardedStore.this.shardsTotal
      }
    }
  }

  private[chompdb] var currentWriterIndex = 0

  def shardFiles = shardWriters map (_.baseFile )
  
  override def put(value: Array[Byte]): Long = {
    val id = shardWriters(currentWriterIndex).put(value)
    currentWriterIndex += 1
    if (currentWriterIndex >= ownedShards.length) currentWriterIndex = 0
    id
  }

  override def close() {
    shardWriters foreach { _.close() }
  }
}