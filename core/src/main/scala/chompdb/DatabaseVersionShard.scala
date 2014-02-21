package chompdb

import f1lesystem.FileSystem

case class DatabaseVersionShard(
  catalog: String,
  database: String,
  version: Long,
  shard: Int 
) {
  def blobFile(rootDir: FileSystem#Dir) = rootDir /+ catalog /+ database /+ version.toString / (shard + ".blob")

  def indexFile(rootDir: FileSystem#Dir) = rootDir /+ catalog /+ database /+ version.toString / (shard + ".index")

  def shardFile(rootDir: FileSystem#Dir) = rootDir /+ catalog /+ database /+ version.toString / (shard + ".shard")
}