package chompdb.server

import chompdb.Database
import chompdb.DatabaseVersionShard
import chompdb.store.VersionedStore

import f1lesystem.LocalFileSystem

abstract class NodeProtocol {
  val chomp: Chomp

  // CLIENT-SIDE
  def availableShards: Node => Set[DatabaseVersionShard]

  // SERVER-SIDE
  def localShards(): Set[DatabaseVersionShard] = chomp
    .databases
    .map { chomp.localDB(_) }
    .flatMap { db => db
      .versions
      .flatMap { v => db.retrieveShards(v) }
    }
    .toSet

  def serveVersion(db: Database, version: Option[Long]) {
    chomp.serveVersion(db, version)
  }
}