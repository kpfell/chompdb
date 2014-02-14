package chompdb.server

import chompdb._
import chompdb.store.{ FileStore, ShardedWriter, VersionedStore }
import f1lesystem.FileSystem
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

case class DatabaseNotFoundException(smth: String) extends Exception
case class DatabaseNotServedException(smth: String) extends Exception
case class ShardsNotFoundException(smth: String) extends Exception
case class VersionNotFoundException(smth: String) extends Exception

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
        .getOrElse { throw new DatabaseNotFoundException("Database $database$ not found.") }

      val numShards = chomp.numShardsPerVersion getOrElse (
        (blobDatabase, version),
        throw new ShardsNotFoundException("Shards for database $blobDatabase.name$ version $version$ not found.")
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
  val databases: Seq[Database]
  val localNode: Node
  val nodes: Map[Node, Endpoint]
  val nodeAlive: NodeAlive
  val replicationFactor: Int
  val replicationBeforeVersionUpgrade: Int
  val maxVersions: Int
  val maxDownloadRetries: Int
  val executor: ScheduledExecutorService
  val databaseUpdateFreq: Duration
  val nodesAliveFreq: Duration
  val nodesContentFreq: Duration
  val servingVersionsFreq: Duration
  val rootDir: FileSystem#Dir

  lazy val hashRing = new HashRing(Chomp.this)

  @transient var availableShards = Set.empty[DatabaseVersionShard]

  @transient var servingVersions = Map.empty[Database, Option[Long]]
  @transient var numShardsPerVersion = Map.empty[(Database, Long), Int]
  
  @transient var nodesAlive = Map.empty[Node, Boolean]
  @transient var nodesContent = Map.empty[Node, Set[DatabaseVersionShard]]

  def nodeProtocol: Map[Node, NodeProtocol]

  def serializeMapReduce[T, U](mapReduce: MapReduce[T, U]): String

  def deserializeMapReduce(mapReduce: String): MapReduce[ByteBuffer, _]

  def serializeMapReduceResult(result: Any): Array[Byte]

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

        if (Chomp.this.localDB(database).versionedStore.shardMarker(version, basename.toInt).exists)
          availableShards = availableShards +
            DatabaseVersionShard(database.catalog.name, database.name, version, basename.toInt)
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

    def copy(from: FileSystem#File, to: FileSystem#File) {
      from.readAsReader { reader => 
        to.write(reader, from.size)
      }
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
      .getOrElse { throw new DatabaseNotFoundException("Database $database$ not found.") }

    val servedVersion = servingVersions getOrElse (
      blobDatabase,
      throw new DatabaseNotServedException("Database $blobDatabase.name$ not currently being served.")
    )

    val version = servedVersion getOrElse (
      throw new VersionNotFoundException("Shards for database $blobDatabase.name$ version $version$ not found.")
    )

    val numShards = numShardsPerVersion getOrElse (
      (blobDatabase, version),
      throw new ShardsNotFoundException("Shards for database $blobDatabase.name$ version $version$ not found.")
    )

    val keysToNodes = partitionKeys(keys, blobDatabase, version, numShards)
    
    // unfortunately, scala's parallel collections don't catch Errors, so let's do
    // ourselves a favor and wrap them so they bubble back up to the client 
    def wrapErrors[T](f: => T) = {
      try f
      catch { case e: Error => 
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

  def partitionKeys(keys: Seq[Long], blobDatabase: Database, version: Long, numShards: Int): Map[Node, Seq[Long]] = {
    val keyToNodesServingShard = keys 
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

        (key, nodesServingShard)
      }

    var nodesToKeys = Map.empty[Node, Seq[Long]]

    for (pair <- keyToNodesServingShard) {
      val key = pair._1
      var assignedNode: Option[Node] = None

      breakable { for (node <- pair._2) {
        if (!nodesToKeys.contains(node)) {
          assignedNode = Some(node)
          break
        } 
        else if (assignedNode == None) assignedNode = Some(node)
        else if (nodesToKeys.getOrElse(node, Seq.empty).size < nodesToKeys.getOrElse(assignedNode.get, Seq.empty).size)
          assignedNode = Some(node)
      } }

      if (nodesToKeys.contains(assignedNode.get)) {
        val nodeKeys = nodesToKeys(assignedNode.get)
        nodesToKeys = nodesToKeys + (assignedNode.get -> (nodeKeys :+ key))
      } else nodesToKeys = nodesToKeys + (assignedNode.get -> Seq(key))
    }

    nodesToKeys
  }

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

    val expiredVersions = Chomp.this
      .localDB(database)
      .versionedStore
      .versions
      .dropRight(maxVersions)

    expiredVersions foreach { version => 
      Chomp.this
        .localDB(database)
        .versionedStore
        .deleteVersion(version)
    }
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