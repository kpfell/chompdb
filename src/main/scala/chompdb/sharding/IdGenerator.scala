package chompdb.sharding

import java.util.concurrent.atomic.AtomicLong

class IdGenerator(val shards: Sharded) {
  private[this] final val id = new AtomicLong(shards.shardsIndex)
  def lastId = id.get - shards.shardsTotal
  def nextId() = id.getAndAdd(shards.shardsTotal)
}
