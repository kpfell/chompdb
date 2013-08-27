package chompdb

import f1lesystem.FileSystem
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

object Store {
  trait Reader extends java.io.Closeable {
    def get(key: Long): Option[Array[Byte]]
  }

  trait Writer extends java.io.Closeable {
    def put(value: Array[Byte]): Long
  }
}

trait Splits {
  val splitIndex: Int
  val splitFactor: Int
}

trait Sharded {
  val shardsIndex: Int
  val shardsTotal: Int
  
  def split(index: Int, splits: Int) = new Sharded {
    val shardsIndex = (Sharded.this.shardsIndex * splits) + index
    val shardsTotal = (Sharded.this.shardsTotal * splits)
    override def toString = s"SplitSharded(shardsIndex=$shardsIndex, shardsTotal=$shardsTotal"
  }
}

private[chompdb] class NextId(val shards: Sharded) {
  private[this] final val id = new AtomicLong(shards.shardsIndex)
  def lastId = id.get - shards.shardsTotal
  def nextId() = id.getAndAdd(shards.shardsTotal)
}
