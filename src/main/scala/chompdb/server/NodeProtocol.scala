package chompdb.server

import chompdb.Database
import chompdb.DatabaseVersionShard
import chompdb.store.VersionedStore

import f1lesystem.LocalFileSystem

abstract class NodeProtocol {
  val chomp: Chomp

  // CLIENT-SIDE
  def allAvailableShards: Node => Set[DatabaseVersionShard]
  def availableShards: (Node, Database) => Set[DatabaseVersionShard]

  // SERVER-SIDE
  def allLocalShards(): Set[DatabaseVersionShard] = chomp
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

  def switchServedVersion(db: Database) {
    // Determine latest Database version available locally
    val latestLocalDatabaseVersion = chomp
      .localDB(db)
      .mostRecentVersion

    // Query other nodes to see if their latest Database versions are the same
    val latestRemoteDatabaseVersions = chomp
      .nodes
      .keys
      .map { n => availableShards(n, db) }

      // If not, and there is a newer version, download that version
    // If so, query the other nodes for their Database versions
    // Create a map of shard -> count
    // If count for each shard is >= replicationBeforeVersionUpgrade, begin serving the latest local version
    // Send command to other nodes for them to switch to the latest local version
  }
}