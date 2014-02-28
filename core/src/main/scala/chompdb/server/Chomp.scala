package chompdb.server

import chompdb._
import chompdb.store.{ FileStore, ShardedWriter, VersionedStore }
import chompdb.util.Logger
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
    override def availableShards(catalog: String, database: String): Set[VersionShard] = {
      chomp.log.debug(s"Returning shards available for $catalog / $database on $node $chomp")
      chomp
        .availableShards
        .filter { _.database == database }
        .map { dbvs => (dbvs.version, dbvs.shard) }
    }

    override def mapReduce(catalog: String, database: String, version: Long, 
        ids: Seq[Long], mapReduce: String): Array[Byte] = {
      chomp.log.debug(s"Beginning mapReduce $mapReduce over $catalog / $database / $version on $node $chomp")
      val mr = chomp.deserializeMapReduce(mapReduce).asInstanceOf[MapReduce[ByteBuffer, Any]]

      val blobDatabase = chomp.databases
        .find { db => db.catalog.name == catalog && db.name == database }
        .getOrElse { 
          chomp.log.error(
            s"Database $database not found on $node $chomp",
            throw new DatabaseNotFoundException("Database $database not found on $node $chomp") 
          )
          throw new DatabaseNotFoundException("Database $database not found on $node $chomp")
        }

      chomp.log.debug(s"Database $blobDatabase.name located locally for mapReduce $mapReduce over $catalog / $database / $version on $node $chomp")

      val numShards = chomp.numShardsPerVersion getOrElse (
        (blobDatabase, version),
        {
          chomp.log.error(
            s"Shards for database $blobDatabase.name version $version not found on $node $chomp",
            throw new DatabaseNotFoundException("Shards for database $blobDatabase.name version $version not found.")
          )
          throw new ShardsNotFoundException("Shards for database $blobDatabase.name version $version not found.")
        }
      )

      chomp.log.debug(s"numShards retrieved from numShardsPerVersion for mapReduce $mapReduce over $catalog / $database / $version on $node $chomp")

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

      chomp.log.debug(s"Determined result for mapReduce $mapReduce over $catalog / $database / $version on $node $chomp")

      chomp.log.info(s"Returning serialized result for mapReduce $mapReduce over $catalog / $database / $version")
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
  // Logger for debugging and status messages
  val log: Logger
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
    log.info(s"Starting chomp $Chomp.this on node $localNode ...")
    hashRing.initialize(nodes keySet)

    log.debug(s"Scheduling all database updates for $localNode $Chomp.this")
    for (database <- databases) {
      scheduleDatabaseUpdate(databaseUpdateFreq, database)
    }
    log.debug(s"Scheduled all database updates for $localNode $Chomp.this")

    log.debug(s"Initially purging inconsistent shards for $localNode $Chomp.this")
    purgeInconsistentShards()
    log.debug(s"Initially purged inconsistent shards for $localNode $Chomp.this")
    
    initializeAvailableShards()
    initializeServingVersions()
    initializeNumShardsPerVersion()
    scheduleNodesAlive(nodesAliveFreq)
    scheduleNodesContent(nodesContentFreq)
    scheduleServingVersions(servingVersionsFreq)

    log.info(s"Chomp $Chomp.this running on node $localNode ...")
  }

  def downloadDatabaseVersion(database: Database, version: Long) {
    log.info(s"Updating database $database.name version $version on $localNode $Chomp.this")

    val remoteDir = database.versionedStore.versionPath(version)
    val remoteVersionMarker = database.versionedStore.versionMarker(version)

    // TODO: This "fails" silently if the version marker does not exist
    if (remoteVersionMarker.exists) {
      log.debug(s"$Chomp.this $localNode detected remote versionMarker for database $database.name version $version")

      val localDB = Chomp.this.localDB(database)

      // TODO: What does createVersion do if there already exists a version there?
      val localDir = localDB.versionedStore.createVersion(version)

      // TODO: This "fails" silently if the number of max retries is reached.
      localDB.versionedStore.deleteIncompleteShards(version)

      log.debug(s"Copying shards for database $database.name version $version from $database.root to $localDB.root on $localNode $Chomp.this")
      copyShards(remoteDir, localDir, 0) foreach { numRetries => 
        if (numRetries < maxDownloadRetries) {
          log.debug(s"Beginning attempt $numRetries to copy shards for database $database.name version $version from $database.root to $localDB.root on $localNode $Chomp.this")
          localDB.versionedStore.deleteIncompleteShards(version)
          copyShards(remoteDir, localDir, numRetries)
          log.debug(s"Completed attempt $numRetries to copy shards for database $database.name version $version from $database.root to $localDB.root on $localNode $Chomp.this")
        }
        else localDB.versionedStore.deleteIncompleteShards(version)
      }

      log.debug(s"Copying version file for database $database.name version $version from $database.root to $localDB.root on $localNode $Chomp.this")
      val vm = database.versionedStore.versionMarker(version)
      copy(vm, localDB.versionedStore.root / vm.filename)
    }

    def copyShards(remoteVersionDir: FileSystem#Dir, localVersionDir: FileSystem#Dir, numRetries: Int): Option[Int] = {
      log.debug(s"Beginning to copy shards from $remoteVersionDir to $localVersionDir on $localNode $Chomp.this")

      val remoteBasenamesToDownload = remoteVersionDir
        .listFiles
        .map { _.basename }
        .filter { basename => (basename forall Character.isDigit) && 
          (hashRing.getNodesForShard(basename.toInt) contains localNode) 
        }
        .toSet

      log.debug(s"Determined remoteBasenamesToDownload from $remoteVersionDir to $localVersionDir on $localNode $Chomp.this")

      for (basename <- remoteBasenamesToDownload) {
        log.debug(s"Attempting to copy shard files for $basename from $remoteVersionDir to $localVersionDir on $localNode $Chomp.this")
        copyShardFiles(basename, remoteVersionDir, localVersionDir)
        log.debug(s"Completed attempt to copy shard files for $basename from $remoteVersionDir to $localVersionDir on $localNode $Chomp.this")
      }

      def copyShardFiles(basename: String, remoteVersionDir: FileSystem#Dir, localVersionDir: FileSystem#Dir) {
        val shard = DatabaseVersionShard(database.catalog.name, database.name, version, basename.toInt)

        val blobFile = shard.blobFile(database.versionedStore)
        log.debug(s"Attempting to copy blobFile for $basename from $remoteVersionDir to $localVersionDir on $localNode $Chomp.this")
        copy(blobFile, localVersionDir / blobFile.filename)
        log.debug(s"Completed attempt to copy blobFile for $basename from $remoteVersionDir to $localVersionDir on $localNode $Chomp.this")

        val indexFile = shard.indexFile(database.versionedStore)
        log.debug(s"Attempting to copy blobFile for $basename from $remoteVersionDir to $localVersionDir on $localNode $Chomp.this")        
        copy(indexFile, localVersionDir / indexFile.filename)
        log.debug(s"Completed attempt to copy blobFile for $basename from $remoteVersionDir to $localVersionDir on $localNode $Chomp.this")        

        if (shard.blobFile(localDB(database).versionedStore).exists && shard.indexFile(localDB(database).versionedStore).exists) {
          log.debug(s"BlobFile and indexFile detected locally for database $database.name version $version shard $basename on $localNode $Chomp.this")        
          Chomp.this.localDB(database).versionedStore.succeedShard(version, basename.toInt)
        }

        if (Chomp.this.localDB(database).versionedStore.shardMarker(version, basename.toInt).exists) {
          log.debug(s"ShardMarker detected locally for database $database.name version $version shard $basename on $localNode $Chomp.this")
          addAvailableShard(shard)
        }
      }

      val localBasenames = localVersionDir
        .listFiles
        .filter { _.extension == "shard" }
        .map { _.basename }
        .toSet

      log.debug(s"Determined set of localBasenames for $localVersionDir on $localNode $Chomp.this")

      if (remoteBasenamesToDownload == localBasenames) None
      else Some(numRetries + 1)
    }
  }

  private def parMap[T, U](m: Map[T, U]) = {
    val pc = m.par
    val executionContext = ExecutionContext.fromExecutor(executor)
    pc.tasksupport = new ExecutionContextTaskSupport(executionContext)
    pc
  }

  override def mapReduce[T: TypeTag](catalog: String, database: String, keys: Seq[Long], mapReduce: MapReduce[ByteBuffer, T]) = {
    log.info(s"Received mapReduce request for $catalog / $database on $localNode $Chomp.this")

    val blobDatabase = databases
      .find { db => db.catalog.name == catalog && db.name == database }
      .getOrElse { 
        log.error(
          s"Database $database not found on $localNode $Chomp.this",
          throw new DatabaseNotFoundException("Database $database not found.")
        )
        throw new DatabaseNotFoundException("Database $database not found.")
      }

    log.debug(s"Database $database located locally for mapReduce $mapReduce on $localNode $Chomp.this")

    val servedVersion = servingVersions getOrElse (
      blobDatabase,
      {
        log.error(
          s"Database $blobDatabase.name not currently being served on $localNode $Chomp.this",
          throw new DatabaseNotServedException(s"Database $blobDatabase.name not currently being served")
        )
        throw new DatabaseNotServedException(s"Database $blobDatabase.name not currently being served")
      }
    )

    val version = servedVersion getOrElse {
      log.error(
        s"Shards for database $blobDatabase.name version not found on $localNode $Chomp.this",
        throw new VersionNotFoundException(s"Shards for database $blobDatabase.name version not found on $localNode $Chomp.this")
      )
      throw new VersionNotFoundException(s"Shards for database $blobDatabase.name version not found on $localNode $Chomp.this")
    }

    log.debug(s"Version $version of database $database located locally for mapReduce $mapReduce on $localNode $Chomp.this")

    val numShards = numShardsPerVersion getOrElse (
      (blobDatabase, version),
      {
        log.error(
          s"Shards for database $blobDatabase.name version $version not found on $localNode $Chomp.this",
          throw new ShardsNotFoundException(s"Shards for database $blobDatabase.name version $version not found on $localNode $Chomp.this")
        )
        throw new ShardsNotFoundException(s"Shards for database $blobDatabase.name version $version not found.")
      }
    )

    log.debug(s"numShards for database $database.name version $version for mapReduce $mapReduce found on $localNode $Chomp.this")

    val keysToNodes = partitionKeys(keys, blobDatabase, version, numShards)
    
    // unfortunately, scala's parallel collections don't catch Errors, so let's do
    // ourselves a favor and wrap them so they bubble back up to the client 
    def wrapErrors[T](f: => T) = {
      try f
      catch { case e: Exception => 
        log.error(
          s"RuntimeException",
          throw new RuntimeException(e)
        )
        throw new RuntimeException(e)
      }
    }
    
    log.info(s"Performing mapReduce over $catalog / $database for $localNode $Chomp.this")

    parMap(keysToNodes) map { case (node, ids) =>
      wrapErrors {
        val serializedResult = nodeProtocol(node).mapReduce(catalog, database, version, ids, serializeMapReduce(mapReduce))
        deserializeMapReduceResult[T](serializedResult)
      }
    } reduce { (t1, t2) => 
      wrapErrors {
        mapReduce.reduce(t1, t2)
      }
    }
  }

  def partitionKeys(keys: Seq[Long], blobDatabase: Database, version: Long, numShards: Int): Map[Node, Seq[Long]] = {
    log.debug("Partitioning received keys for $blobDatabase.catalog.name / $blobDatabase.name / $version on $localNode $Chomp.this")

    keys
      .map { key => 
        val shard = DatabaseVersionShard(
          blobDatabase.catalog.name, 
          blobDatabase.name, 
          version, 
          (key % numShards).toInt
        )

        val nodesServingShard = nodesContent filter { _._2 contains shard } keySet

        val nodesAssignedShard = hashRing.getNodesForShard(shard.shard)

        val nodesAvailableWithShard = nodesAssignedShard
          .filter { n => (nodesServingShard contains n) && nodesAlive.getOrElse(n, false) }

        (nodesAvailableWithShard.head, key)
      }
      .groupBy(_._1)
      .map { case (node, seq) => (node, seq map { _._2 }) }
  }

  def getNewerVersionNumber(database: Database): Option[Long] = {
    log.debug(s"Getting newer version number for database $database.name on $localNode $Chomp.this")

    database
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
  }

  def localDB(database: Database): Database = new Database (
    new Catalog(database.catalog.name, rootDir),
    database.name
  )

  def initializeAvailableShards() {
    log.debug(s"Initializing available shards for $localNode $Chomp.this")

    availableShards = databases flatMap { db => 
      val local = localDB(db)
      local.versionedStore.versions flatMap { v => local.shardsOfVersion(v) }
    } toSet
  }

  def addAvailableShard(shard: DatabaseVersionShard) {
    log.debug(s"Adding $shard to availableShards on $localNode $Chomp.this")
    availableShards = availableShards + shard
  }

  def purgeInconsistentShards() {
    log.debug(s"Purging inconsistent shards for $localNode $Chomp.this")

    for {
      db <- databases
      val vs = localDB(db).versionedStore
      v <- vs.versions
    } {
      vs.deleteIncompleteShards(v)
    }
  }

  def initializeNumShardsPerVersion() {
    log.debug(s"Initializing number of shards per version for $localNode $Chomp.this")

    numShardsPerVersion = databases
      .map { db => (db, localDB(db).versionedStore.versions) }
      .map { dbvs => dbvs._2 map { v => (dbvs._1, v) } }
      .flatten
      .map { dbv => (dbv._1, dbv._2) -> localDB(dbv._1).versionedStore.numShardsForVersion(dbv._2) }
      .toMap
  }

  def initializeServingVersions() {
    log.debug(s"Initializing serving versions for $localNode $Chomp.this")

    servingVersions = databases
      .map { db => (db, localDB(db).versionedStore.mostRecentVersion) }
      .toMap
  }

  def scheduleDatabaseUpdate(duration: Duration, database: Database) {
    log.debug(s"Scheduling database updates every $duration.length $duration.unit for database $database.name on $localNode $Chomp.this")

    val task: Runnable = new Runnable() {
      def run() {
        updateDatabase(database)
      }
    }

    executor.scheduleWithFixedDelay(task, 0L, duration.toMillis, MILLISECONDS)
  }

  def scheduleNodesAlive(duration: Duration) {
    log.debug(s"Scheduling nodesAlive updates every $duration.length $duration.unit on $localNode $Chomp.this")

    val task: Runnable = new Runnable() {
      def run() {
        updateNodesAlive()
      }
    }

    executor.scheduleWithFixedDelay(task, 0L, duration.toMillis, MILLISECONDS)
  }

  def scheduleNodesContent(duration: Duration) {
    log.debug(s"Scheduling nodesContent updates every $duration.length $duration.unit on $localNode $Chomp.this")

    val task: Runnable = new Runnable() {
      def run() {
        updateNodesContent()
      }
    }

    executor.scheduleWithFixedDelay(task, 0L, duration.toMillis, MILLISECONDS)
  }

  def scheduleServingVersions(duration: Duration) {
    log.debug(s"Scheduling servingVersions updates every $duration.length $duration.unit on $localNode $Chomp.this")

    val task: Runnable = new Runnable() {
      def run() {
        updateServingVersions()
      }
    }

    executor.scheduleWithFixedDelay(task, 0L, duration.toMillis, MILLISECONDS)
  }

  def serveVersion(database: Database, version: Option[Long]) {
    log.debug(s"Serving database $database.name version $version on $localNode $Chomp.this")
    servingVersions = servingVersions + (database -> version)
  }

  def updateDatabase(database: Database) {
    log.debug(s"Updating database $database.name on $localNode $Chomp.this")

    getNewerVersionNumber(database) foreach { version => 
      if (!localDB(database).versionedStore.versionExists(version)) {
        downloadDatabaseVersion(database, version)
      }
    }

    log.debug(s"Deleting expired versions on $localNode $Chomp.this")
    localDB(database)
      .versionedStore
      .cleanup(maxVersions)
  }

  def updateNodesAlive() {
    log.debug(s"Updating nodesAlive on $localNode $Chomp.this")
    nodesAlive = nodes
      .keys
      .map( n => n -> nodeAlive.isAlive(n) )(breakOut)
  }

  def updateNodesContent() {
    log.debug(s"Updating nodesContent on $localNode $Chomp.this")
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
    log.debug(s"Updating servingVersions on $localNode $Chomp.this")

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