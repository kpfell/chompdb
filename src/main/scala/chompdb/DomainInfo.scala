package chompdb

trait DomainInfo {
  val numShards: Int
  val store: Store
  val shardingScheme: ShardingScheme
}
