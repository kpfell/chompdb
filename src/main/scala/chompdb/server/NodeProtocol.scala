package chompdb.server

import chompdb.Database
import chompdb.DatabaseVersionShard
import chompdb.store.VersionedStore

import f1lesystem.LocalFileSystem

import scala.collection._

trait NodeProtocol {
  type VersionShard = (Long, Int)
  def availableShards(catalog: String, database: String): Set[VersionShard]
  def serveVersion(catalog: String, database: String, version: Long): Unit
  // def deserializeMapReduce[T, U](mapReduce: String): MapReduce[T, U]
}