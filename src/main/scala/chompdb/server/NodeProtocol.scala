package chompdb.server

import chompdb.Database
import chompdb.DatabaseVersionShard
import chompdb.store.VersionedStore

import f1lesystem.LocalFileSystem

abstract class NodeProtocol {
  val chomp: Chomp

  // CLIENT-SIDE

  // TODO: Write tests for these?
  def availableShards: (Node, Database) => Set[DatabaseVersionShard]
  def availableVersions: (Node, Database) => Set[Option[Long]]
  def serveVersion: (Node, Database, Option[Long]) => Unit
  def retrieveVersionsServed: Node => Map[Database, Option[Long]]

  // TODO: Verify that every node has some shards for this version before
  // this method is run, and that shards meet minimum replication factor
  def remoteNodesServeVersion(db: Database, v: Option[Long]) { 
    chomp
      .nodes
      .keys
      .foreach { n => serveVersion(n, db, v) }
  }

  def versionShardsPerNode(db: Database, v: Long): Map[Node, Set[DatabaseVersionShard]] = chomp
    .nodes
    .keys
    .map { n => (n, availableShards(n, db).filter(_.version == v)) }
    .toMap

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
    // Verify that the version is available locally
    val versionAvailable =
      if (version.isEmpty) true
      else chomp
        .localDB(db)
        .versionExists(version.get)

    // If so, serve the version
    // If not, initiate a download of the version
    // If the version fails to download, do not begin serving that version
    if (versionAvailable) chomp.serveVersion(db, version)
    else {
      chomp.downloadDatabaseVersion(db, version.get)
      if (chomp.localDB(db).versionExists(version.get)) 
        chomp.serveVersion(db, version)
    }
  }

  def shardsBelowRepFactBeforeUpgrade(vspn: Map[Node, Set[DatabaseVersionShard]]): Set[DatabaseVersionShard] = {
    vspn
      .values
      .toList
      .flatten
      .foldLeft(Map[DatabaseVersionShard, Int]() withDefaultValue 0){
        (s, x) => s + (x -> (1 + s(x)))
      }
      .filter(_._2 < chomp.replicationBeforeVersionUpgrade)
      .keys
      .toSet
  }

  def updateNodesServingVersions() { chomp.nodesServingVersions =
    chomp
      .nodes
      .keys
      .map { n => (n, retrieveVersionsServed(n)) }
      .toMap
  }

  // If enough nodes have the latest version, and we're not serving the latest
  // version, then broadcast move to latest version

  /* IN PROGRESS */
  def switchServedVersion(db: Database) {
    chomp
      .localDB(db)
      .mostRecentVersion
      .foreach { latestLocalDatabaseVersion => // (1)
        // If chomp is not serving db, or if chomp is serving a version of db
        // that is not the latestLocalDatabaseVersion
        if (
          if (chomp.servingVersions.contains(db)) {
            chomp.servingVersions(db).exists(_ != latestLocalDatabaseVersion)
          } else false
        ){
          
          val nodesWithVersionShards = versionShardsPerNode(db, latestLocalDatabaseVersion)

          val numShardsBelowMinReplication = shardsBelowRepFactBeforeUpgrade(nodesWithVersionShards).size

          if (numShardsBelowMinReplication != 0) {
            serveVersion(db, Some(latestLocalDatabaseVersion))
            remoteNodesServeVersion(db, Some(latestLocalDatabaseVersion))
          }
        }    
      }

  }
}