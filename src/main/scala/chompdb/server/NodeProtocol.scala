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
  def availableShardsForVersion: (Node, Database, Long) => Set[DatabaseVersionShard]
  def latestVersion: (Node, Database) => Option[Long]
  def serveVersion: (Node, Database, Long) => Boolean

  def latestRemoteVersions(db: Database): Set[Option[Long]] = chomp
    .nodes
    .keys
    .map { n => latestVersion(n, db) }
    .toSet

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

  def shardsBelowRepFactBeforeUpgrade(db: Database, v: Long) = chomp
    .nodes
    .keys
    .map { n => availableShardsForVersion(n, db, v) }
    .toList
    .flatten
    .foldLeft(Map[DatabaseVersionShard, Int]() withDefaultValue 0){
      (s, x) => s + (x -> (1 + s(x)))
    } 
    .filter(_._2 < chomp.replicationBeforeVersionUpgrade)


  def serveVersion(db: Database, version: Option[Long]) {
    chomp.serveVersion(db, version)
  }

  /* IN PROGRESS */
  def switchServedVersion(db: Database) {
    // TODO: Need to check that the latest version is not already being served
    // Determine latest Database version available locally
    chomp
      .localDB(db)
      .mostRecentVersion
      .foreach { latestLocalDatabaseVersion =>
        val versionGroups = latestRemoteVersions(db)
          .groupBy {
            case None => "none"
            case v if (v.get < latestLocalDatabaseVersion) => "older"
            case v if (v.get == latestLocalDatabaseVersion) => "equal"
            case _ => "newer"
          } 

        // TODO: Other cases
        if (versionGroups.contains("equal") && versionGroups.size == 1) {
          val shardsBelowMinReplication = 
            shardsBelowRepFactBeforeUpgrade(db, latestLocalDatabaseVersion)
          
          if (shardsBelowMinReplication.size == 0) {
            serveVersion(db, Some(latestLocalDatabaseVersion))

            chomp
              .nodes
              .keys
              .foreach { n => serveVersion(n, db, latestLocalDatabaseVersion) }
          }
        }  
      }

  }
}