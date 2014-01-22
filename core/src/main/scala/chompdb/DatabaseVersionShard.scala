package chompdb

case class DatabaseVersionShard(
  catalog: String,
  database: String,
  version: Long,
  shard: Int 
)