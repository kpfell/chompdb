package chompdb.server

import chompdb.Database
import chompdb.DatabaseVersionShard
import chompdb.store.VersionedStore

import f1lesystem.LocalFileSystem

import java.nio.ByteBuffer
import scala.collection._

trait NodeProtocol {
  type VersionShard = (Long, Int)
  def availableShards(catalog: String, database: String): Set[VersionShard]
  def mapReduce(catalog: String, database: String, version: Long,
      ids: Seq[Long], mapReduce: String): Array[Byte]
}