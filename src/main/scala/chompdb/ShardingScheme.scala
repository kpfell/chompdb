package chompdb

import KeyValue._

trait ShardingScheme {
  def shardIndex(shardKey: Key, shardCount: Int): Int
}
