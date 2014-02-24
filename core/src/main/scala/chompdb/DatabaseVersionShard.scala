package chompdb

import chompdb.store.VersionedStore
import f1lesystem.FileSystem

case class DatabaseVersionShard(
  catalog: String,
  database: String,
  version: Long,
  shard: Int 
) {
  def blobFile(versionedStore: VersionedStore): FileSystem#File = versionedStore.root /+ version.toString / (shard + ".blob")

  def indexFile(versionedStore: VersionedStore): FileSystem#File = versionedStore.root /+ version.toString / (shard + ".index")

  def shardFile(versionedStore: VersionedStore): FileSystem#File = versionedStore.root /+ version.toString / (shard + ".shard")
}