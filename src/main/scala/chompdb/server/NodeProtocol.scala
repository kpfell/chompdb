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
  def latestVersion: (Node, Database) => Option[Long]
  def serveVersion: (Node, Database, Long) => Boolean

  // SERVER-SIDE
  def allLocalShards(): Set[DatabaseVersionShard] = chomp
    .databases
    .map { chomp.localDB(_) }
    .flatMap { db => db
      .versions
      .flatMap { v => db.retrieveShards(v) }
    }
    .toSet

  def localShards(db: Database): Set[DatabaseVersionShard] = chomp
    .localDB(db)
    .versions
    .flatMap { v => chomp
      .localDB(db)
      .retrieveShards(v)
    }
    .toSet

  def serveVersion(db: Database, version: Option[Long]) {
    chomp.serveVersion(db, version)
  }

  /* IN PROGRESS */
  def switchServedVersion(db: Database) {
    // Determine latest Database version available locally
    chomp
      .localDB(db)
      .mostRecentVersion
      .foreach { latestLocalDatabaseVersion =>
        // Query other nodes to see if their latest Database versions are the same
        val latestRemoteVersions = chomp
          .nodes
          .keys
          .map { n => latestVersion(n, db) }
          .flatten // ^^ SWITCH TO FLATMAP? ^^
          .filter { v => v == None || v > latestLocalDatabaseVersion }
          // TODO: Replace the v == None comparison

        if (latestRemoteVersions.size > 0) {
          // If not, and there is a newer version, download that version
        } else {
          // If so, query the other nodes for their Database versions
          // Create a map of shard -> count
          val latestRemoteDBVs = chomp
            .nodes
            .keys
            .map { n => availableShards(n, db) }
            .toList
            .flatten
            .foldLeft(Map[DatabaseVersionShard, Int]() withDefaultValue 0){ 
              (s, x) => s + (x -> (1 + s(x)))
            }
            .filter(_._2 < chomp.replicationBeforeVersionUpgrade)
            .size

          if (latestRemoteDBVs == 0) {
            chomp.serveVersion(db, Some(latestLocalDatabaseVersion)) // NEED TO ACCOUNT FOR CASE WHERE LATESTLOCALVERSION IS THE VERSION BEING SERVED
            
            chomp
              .nodes
              .keys
              .map { n => serveVersion(n, db, latestLocalDatabaseVersion) }
          }
        }
      }


    // If count for each shard is >= replicationBeforeVersionUpgrade, begin serving the latest local version
    // Send command to other nodes for them to switch to the latest local version
  }
}