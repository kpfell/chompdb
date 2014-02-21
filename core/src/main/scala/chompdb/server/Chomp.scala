package chompdb.server

import chompdb._
import chompdb.store.{ FileStore, ShardedWriter, VersionedStore }
import f1lesystem.FileSystem
import f1lesystem.FileSystem.copy
import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.ScheduledExecutorService
import java.util.Properties
import scala.collection._
import scala.collection.parallel.ExecutionContextTaskSupport
import scala.concurrent.ExecutionContext
import scala.util.control.Breaks._
import scala.concurrent.duration._
import scala.reflect.runtime.universe._

case class DatabaseNotFoundException(msg: String) extends Exception
case class DatabaseNotServedException(msg: String) extends Exception
case class ShardsNotFoundException(msg: String) extends Exception
case class VersionNotFoundException(msg: String) extends Exception

object Chomp {
  class LocalNodeProtocol(node: Node, chomp: Chomp) extends NodeProtocol {
    override def availableShards(catalog: String, database: String): Set[VersionShard] =
      chomp
        .availableShards
        .filter(_.database == database)
        .map { dbvs => (dbvs.version, dbvs.shard) }

    override def mapReduce(catalog: String, database: String, version: Long, 
        ids: Seq[Long], mapReduce: String): Array[Byte] = {
      
      val mr = chomp.deserializeMapReduce(mapReduce).asInstanceOf[MapReduce[ByteBuffer, Any]]

      val blobDatabase = chomp.databases
        .find { db => db.catalog.name == catalog && db.name == database }
        .getOrElse { throw new DatabaseNotFoundException("Database $database not found.") }

      val numShards = chomp.numShardsPerVersion getOrElse (
        (blobDatabase, version),
        throw new ShardsNotFoundException("Shards for database $blobDatabase.name version $version not found.")
      )

      val result = parSeq(ids) map { id => 
        val shard = DatabaseVersionShard(catalog, database, version, (id % numShards).toInt)

        val reader = new FileStore.Reader {
          val baseFile: FileSystem#File = chomp
            .localDB(blobDatabase)
            .versionedStore
            .shardMarker(shard.version, shard.shard)
        }

        val blob = reader.get(id)
        reader.close()

        val bbBlob = ByteBuffer.wrap(blob)
        mr.map(bbBlob)

      } reduce { mr.reduce(_, _) }

      chomp.serializeMapReduceResult(result)
    }

    private def parSeq[T](s: Seq[T]) = {
      val pc = s.par
      val executionContext = ExecutionContext.fromExecutor(chomp.executor)
      pc.tasksupport = new ExecutionContextTaskSupport(executionContext)
      pc
    }
  }
}

abstract class Chomp extends SlapChop {
  // Databases on secondary filesystem
  val databases: Seq[Database]
  // Node on which this Chomp instance resides
  val localNode: Node
  // Map of Nodes in the network to their corresponding Endpoints
  val nodes: Map[Node, Endpoint]
  // NodeAlive implementation for verifying availability of Nodes
  val nodeAlive: NodeAlive
  // Number of times a shard should be replicated across the network 
  val replicationFactor: Int
  // Number of times a shard must be replicated across the network before
  // this Chomp initiates queries against its version
  val replicationBeforeVersionUpgrade: Int
  // Maximum number of versions that will exist simultaneously
  val maxVersions: Int
  // Maximum number of times the Chomp will attempt to download
  // a DatabaseVersionShard
  val maxDownloadRetries: Int
  // ScheduledExecutorService for scheduling recurring database updates,
  // network status updates, etc.
  val executor: ScheduledExecutorService
  // Frequency with which the Chomp should check for a new database version
  val databaseUpdateFreq: Duration
  // Frequency with which the Chomp should update the availability status of 
  // Nodes in the network
  val nodesAliveFreq: Duration
  // Frequency with which the Chomp should update its mappings of Nodes to
  // shards being served
  val nodesContentFreq: Duration
  // Frequency with which the Chomp should update its mappings of Databases
  // to versions that queries should be initiated against
  val servingVersionsFreq: Duration
  // Root directory for this Chomp
  val rootDir: FileSystem#Dir

  // HashRing instance for consistent hashing
  lazy val hashRing = new HashRing(Chomp.this)

  // Set of shards available locally on current Node
  @transient var availableShards = Set.empty[DatabaseVersionShard]

  // Map of Databases to versions that queries should be initiated against
  @transient var servingVersions = Map.empty[Database, Option[Long]]
  // Map of Databases and versions to the number of shards in that version
  @transient var numShardsPerVersion = Map.empty[(Database, Long), Int]
  
  // Map of Nodes to whether they are available
  @transient var nodesAlive = Map.empty[Node, Boolean]
  // Map of Nodes to the set of shards they each have locally
  @transient var nodesContent = Map.empty[Node, Set[DatabaseVersionShard]]

  // Map of Nodes to NodeProtocols for interacting with them
  def nodeProtocol: Map[Node, NodeProtocol]

  // Method for serializing MapReduce queries
  def serializeMapReduce[T, U](mapReduce: MapReduce[T, U]): String

  // Method for deserializing MapReduce queries
  def deserializeMapReduce(mapReduce: String): MapReduce[ByteBuffer, _]

  // Method for serializing MapReduce results
  def serializeMapReduceResult(result: Any): Array[Byte]

  // Method for deserializing MapReduce results
  def deserializeMapReduceResult[T: TypeTag](result: Array[Byte]): T

  def run() {
    hashRing.initialize(nodes.keys.toSet)

    for (database <- databases) {
      scheduleDatabaseUpdate(databaseUpdateFreq, database)
    }

    purgeInconsistentShards()
    initializeAvailableShards()
    initializeServingVersions()
    initializeNumShardsPerVersion()
    scheduleNodesAlive(nodesAliveFreq)
    scheduleNodesContent(nodesContentFreq)
    scheduleServingVersions(servingVersionsFreq)
  }

  def downloadDatabaseVersion(database: Database, version: Long) = {
    val remoteDir = database.versionedStore.versionPath(version)
    val remoteVersionMarker = database.versionedStore.versionMarker(version)

    // TODO: This "fails" silently if the version marker does not exist
    if (remoteVersionMarker.exists) {
      val localDB = Chomp.this.localDB(database)

      // TODO: What does createVersion do if there already exists a version there?
      val localDir = localDB.versionedStore.createVersion(version)

      // TODO: This "fails" silently if the number of max retries is reached.
      deleteIncompleteShards(localDir)

      copyShards(remoteDir, localDir, 0) foreach { numRetries => 
        if (numRetries < maxDownloadRetries) {
          deleteIncompleteShards(localDir)
          copyShards(remoteDir, localDir, numRetries)
        }
        else deleteIncompleteShards(localDir)
      }

      copyVersionFile(database.versionedStore.versionMarker(version), localDB.versionedStore.root)
    }

    def deleteIncompleteShards(localVersionDir: FileSystem#Dir) {
      localVersionDir
        .listFiles
        .filter { _.basename forall Character.isDigit }
        .foreach { f =>
          if (!(localVersionDir / (f.basename + ".shard")).exists) f.delete()
        }
    }

    def copyShards(remoteVersionDir: FileSystem#Dir, localVersionDir: FileSystem#Dir, 
        numRetries: Int): Option[Int] = {
      val remoteBasenamesToDownload = remoteVersionDir
        .listFiles
        .map { _.basename }
        .filter { basename => (basename forall Character.isDigit) && 
          (hashRing.getNodesForShard(basename.toInt) contains localNode) 
        }
        .toSet

      for (basename <- remoteBasenamesToDownload) {
        copyShardFiles(basename, remoteVersionDir, localVersionDir)
      }

      def copyShardFiles(basename: String, remoteVersionDir: FileSystem#Dir, 
          localVersionDir: FileSystem#Dir) {
        val blobFile = remoteVersionDir / (basename + ".blob")
        copy(blobFile, localVersionDir / blobFile.filename)

        val indexFile = remoteVersionDir / (basename + ".index")
        copy(indexFile, localVersionDir / indexFile.filename)

        if ((localVersionDir / blobFile.filename).exists && (localVersionDir / indexFile.filename).exists)
          Chomp.this.localDB(database).versionedStore.succeedShard(version, basename.toInt)

        if (Chomp.this.localDB(database).versionedStore.shardMarker(version, basename.toInt).exists) {
          val shard = DatabaseVersionShard(database.catalog.name, database.name, version, basename.toInt)
          addAvailableShard(shard)
        }
      }

      val localBasenames = localVersionDir
        .listFiles
        .filter { _.extension == "shard" }
        .map { _.basename }
        .toSet

      if (remoteBasenamesToDownload == localBasenames) None
      else Some(numRetries + 1)
    }

    def copyVersionFile(versionRemotePath: FileSystem#File, versionLocalDir: FileSystem#Dir) {
      copy(versionRemotePath, versionLocalDir / versionRemotePath.filename)
    }
  }

  private def parMap[T, U](m: Map[T, U]) = {
    val pc = m.par
    val executionContext = ExecutionContext.fromExecutor(executor)
    pc.tasksupport = new ExecutionContextTaskSupport(executionContext)
    pc
  }

  override def mapReduce[T: TypeTag](catalog: String, database: String, keys: Seq[Long], mapReduce: MapReduce[ByteBuffer, T]) = {
    val blobDatabase = databases
      .find { db => db.catalog.name == catalog && db.name == database }
      .getOrElse { throw new DatabaseNotFoundException("Database $database not found.") }

    val servedVersion = servingVersions getOrElse (
      blobDatabase,
      throw new DatabaseNotServedException("Database $blobDatabase.name not currently being served.")
    )

    val version = servedVersion getOrElse (
      throw new VersionNotFoundException("Shards for database $blobDatabase.name version $version not found.")
    )

    val numShards = numShardsPerVersion getOrElse (
      (blobDatabase, version),
      throw new ShardsNotFoundException("Shards for database $blobDatabase.name version $version not found.")
    )

    val keysToNodes = partitionKeys(keys, blobDatabase, version, numShards)
    
    // unfortunately, scala's parallel collections don't catch Errors, so let's do
    // ourselves a favor and wrap them so they bubble back up to the client 
    def wrapErrors[T](f: => T) = {
      try f
      catch { case e: Exception => 
        throw new RuntimeException(e)
      }
    }
    
    parMap(keysToNodes) map { case (node, ids) =>
      wrapErrors{
        val serializedResult = nodeProtocol(node).mapReduce(catalog, database, version, ids, serializeMapReduce(mapReduce))
        deserializeMapReduceResult[T](serializedResult)
      }
    } reduce { (t1, t2) => 
      wrapErrors {
        mapReduce.reduce(t1, t2)
      }
    }
  }

  def partitionKeys(keys: Seq[Long], blobDatabase: Database, version: Long, 
      numShards: Int): Map[Node, Seq[Long]] = keys
    .map { key => 
      val shard = DatabaseVersionShard(
        blobDatabase.catalog.name, 
        blobDatabase.name, 
        version, 
        (key % numShards).toInt
      )

      val nodesServingShard = nodesContent
        .filter { _._2 contains shard }
        .keys
        .toSet

      val nodesAssignedShard = hashRing.getNodesForShard(shard.shard)

      val nodesAvailableWithShard = nodesAssignedShard
        .filter { n => (nodesServingShard contains n) && nodesAlive.getOrElse(n, false) }

      (nodesAvailableWithShard.head, key)
    }
    .groupBy(_._1)
    .map { case (node, seq) => (node, seq map { _._2 }) }

  def getNewVersionNumber(database: Database): Option[Long] = database
    .versionedStore
    .mostRecentVersion
    .flatMap { latestRemoteVersion => 
      localDB(database).versionedStore.mostRecentVersion match {
        case Some(latestLocalVersion) =>
          if (latestRemoteVersion > latestLocalVersion) Some(latestRemoteVersion)
          else None
        case None => Some(latestRemoteVersion)
      }
    }

  def localDB(database: Database): Database = new Database(
    new Catalog(database.catalog.name, rootDir),
    database.name
  )
  
  def initializeAvailableShards() {
    availableShards = databases
      .map { db => localDB(db).versionedStore.versions 
        .map { v => localDB(db).shardsOfVersion(v) } 
      }
      .toSet
      .flatten
      .flatten
  }

  def addAvailableShard(shard: DatabaseVersionShard) {
    availableShards = availableShards + shard
  }

  def purgeInconsistentShards() {
    def isInconsistentShard(db: Database, v: Long, f: FileSystem#File): Boolean = {
      (f.extension == "blob" || f.extension == "index") && 
        !(db.versionedStore.versionPath(v) / (f.basename + ".shard")).exists
    }

    databases
      .map { db => localDB(db).versionedStore.versions
        .map { v => localDB(db).versionedStore.versionPath(v)
          .listFiles
          .filter { f => isInconsistentShard(localDB(db), v, f) }
          .foreach { f => f.delete() }
        }
      }
  }

  def initializeNumShardsPerVersion() {
    numShardsPerVersion = databases
      .map { db => (db, localDB(db).versionedStore.versions) }
      .map { dbvs => dbvs._2 map { v => (dbvs._1, v) } }
      .flatten
      .map { dbv => (dbv._1, dbv._2) -> localDB(dbv._1).versionedStore.numShardsForVersion(dbv._2) }
      .toMap
  }

  def initializeServingVersions() {
    servingVersions = databases
      .map { db => (db, localDB(db).versionedStore.mostRecentVersion) }
      .toMap
  }

  def scheduleDatabaseUpdate(duration: Duration, database: Database) = {
    val task: Runnable = new Runnable() {
      def run() {
        updateDatabase(database)
      }
    }

    executor.scheduleAtFixedRate(task, 0L, duration.toMillis, MILLISECONDS)
  }

  def scheduleNodesAlive(duration: Duration) = {
    val task: Runnable = new Runnable() {
      def run() {
        updateNodesAlive()
      }
    }

    executor.scheduleWithFixedDelay(task, 0L, duration.toMillis, MILLISECONDS)
  }

  def scheduleNodesContent(duration: Duration) = {
    val task: Runnable = new Runnable() {
      def run() {
        updateNodesContent()
      }
    }

    executor.scheduleAtFixedRate(task, 0L, duration.toMillis, MILLISECONDS)
  }

  def scheduleServingVersions(duration: Duration) = {
    val task: Runnable = new Runnable() {
      def run() {
        updateServingVersions()
      }
    }

    executor.scheduleAtFixedRate(task, 0L, duration.toMillis, MILLISECONDS)
  }

  def serveVersion(database: Database, version: Option[Long]) = {
    servingVersions = servingVersions + (database -> version)
  }

  def updateDatabase(database: Database) {
    getNewVersionNumber(database) foreach { version => 
      if (!localDB(database).versionedStore.versionExists(version))
        downloadDatabaseVersion(database, version)
    }

    localDB(database)
      .versionedStore
      .cleanup(maxVersions)
  }

  def updateNodesAlive() {
    nodesAlive = nodes
      .keys
      .map( n => n -> nodeAlive.isAlive(n) )(breakOut)
  }

  def updateNodesContent() {
    nodesContent = nodes
      .keys
      .map { n => n -> databases
        .map { db => nodeProtocol(n)
          .availableShards(db.catalog.name, db.name)
          .map { vs => DatabaseVersionShard(db.catalog.name, db.name, vs._1, vs._2) }
        }
        .flatten
        .toSet
      }
      .toMap
  }

  def updateServingVersions() {
    val latestLocalVersions = databases
      .map { db => (db.catalog.name, db.name) -> localDB(db).versionedStore.mostRecentVersion }
      .toMap

    val latestShardsInNetwork = nodesContent
      .values
      .toList
      .flatten
      .filter { s => s.version == latestLocalVersions.getOrElse((s.catalog, s.database), -100) }
    
    val shardsInNetworkByDBV = latestShardsInNetwork
      .groupBy { s => (s.catalog, s.database, s.version) }
      .map { case ((c, db, version), shardList) =>  ((c, db, version), shardList groupBy { _.shard }) }
      .toMap

    val dbvToShardCounts = shardsInNetworkByDBV
      .map { case ((c, db, version), shardMap) => (c, db, version) -> shardMap
        .values
        .map { s => s.size }
        .filter { _ < replicationBeforeVersionUpgrade }
      }
    
    dbvToShardCounts foreach { case ((c, db, version), shardCounts) =>
      if (shardCounts.size == 0) {
        val servedDb = databases find { d => d.catalog.name == c && d.name == db }
        if (!servedDb.isEmpty) serveVersion(servedDb.get, Some(version))
      }
    }
  }
}