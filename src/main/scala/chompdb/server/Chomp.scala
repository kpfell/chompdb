package chompdb.server

import chompdb._
import chompdb.store.{ FileStore, ShardedWriter, VersionedStore }
import f1lesystem.FileSystem
import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.ScheduledExecutorService
import java.util.Properties
import scala.collection._

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

		override def serveVersion(catalog: String, database: String, version: Long) {
			chomp
				.databases
				.find(_.name == database)
				.foreach { db => chomp.serveVersion(db, Some(version)) }
		}

		override def get(catalog: String, database: String, key: Long): ByteBuffer = {
			chomp.getBlob(catalog, database, key)
		}
	}
}

abstract class Chomp() {
	val databases: Seq[Database]
	val localNode: Node
	val nodes: Map[Node, Endpoint] 
	val nodeAlive: NodeAlive
	val replicationFactor: Int
	val replicationBeforeVersionUpgrade: Int // TODO: Come up with a better name
	val maxDownloadRetries: Int
	val executor: ScheduledExecutorService
	val fs: FileSystem
	val rootDir: FileSystem#Dir

	@transient private[server] var availableShards = Set.empty[DatabaseVersionShard]

	@transient var servingVersions = Map.empty[Database, Option[Long]]
	@transient var numShardsPerVersion = Map.empty[(Database, Long), Int]
	@transient var nodesServingVersions = Map.empty[Node, Map[Database, Option[Long]]]

	@transient var nodesAlive = Map.empty[Node, Boolean]
	@transient var nodesContent = Map.empty[Node, Set[DatabaseVersionShard]]

	def nodeProtocol: Map[Node, NodeProtocol]

	def serializeMapReduce[T, U](mapReduce: MapReduce[T, U]): String

	def main(args: Array[String]) {
		initializeAvailableShards()
		purgeInconsistentShards()
		initializeServingVersions()
	}

	def downloadDatabaseVersion(database: Database, version: Long) = {
		val remoteDir = database.versionedStore.versionPath(version)
		val remoteVersionMarker = database.versionedStore.versionMarker(version)

		// TODO: This "fails" silently if the version does not exist.
		if (remoteVersionMarker.exists) {
			val rvmInput = new FileInputStream(remoteVersionMarker.fullpath)
			val props = new Properties()
			props.load(rvmInput)
			rvmInput.close()

			val shardIndex = 
				if (props contains "highestShardIndex") props.getProperty("highestShardIndex").toInt + 1
				else 0

			if (props contains "highestShardIndex") {				
				props.setProperty("highestShardIndex", shardIndex.toString)
				val rvmOutput = new FileOutputStream(remoteVersionMarker.fullpath)
				props.store(rvmOutput, null)
				rvmOutput.close()

				Chomp.this.localDB(database).versionedStore.succeedShardIndex(version, shardIndex)
			} else {
				props.put("highestShardIndex", shardIndex.toString)
				val rvmOutput = new FileOutputStream(remoteVersionMarker.fullpath)
				props.store(rvmOutput, null)
				rvmOutput.close()

				Chomp.this.localDB(database).versionedStore.succeedShardIndex(version, shardIndex)
			}

			val localDB = Chomp.this.localDB(database)
			// TODO: What does createVersion do if there already exists a version there?
			val localDir = localDB.versionedStore.createVersion(version)

			// TODO: This "fails" silently if the number of max retries is reached
			deleteIncompleteShards(localDir)
			copyShards(remoteDir, localDir, 0, shardIndex) foreach { numRetries =>
				if (numRetries < maxDownloadRetries) {
					deleteIncompleteShards(localDir)
					copyShards(remoteDir, localDir, numRetries, shardIndex)
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

		def copyShards(remoteVersionDir: FileSystem#Dir, localVersionDir: FileSystem#Dir, numRetries: Int, shardIndex: Int): Option[Int] = {
			val nodeCount = nodes.size

			val remoteBasenamesToDownload = remoteVersionDir
				.listFiles
				.map { _.basename }
				.filter { basename => (basename forall Character.isDigit) && (basename.toInt % nodeCount == shardIndex) }
				.filter { basename => !(localVersionDir / (basename + ".shard")).exists }
				.toSet

			for (basename <- remoteBasenamesToDownload) {
				copyShardFiles(basename, remoteVersionDir, localVersionDir)
			}

			def copyShardFiles(basename: String, remoteVersionDir: FileSystem#Dir, localVersionDir: FileSystem#Dir) {
				val blobFile = remoteVersionDir / (basename + ".blob")
						copy(blobFile, localVersionDir / blobFile.filename)

				val indexFile = remoteVersionDir / (basename + ".index")
				copy(indexFile, localVersionDir / indexFile.filename)

				if ((localVersionDir / blobFile.filename).exists && (localVersionDir / indexFile.filename).exists)
					Chomp.this.localDB(database).versionedStore.succeedShard(version, basename.toInt)

				if (Chomp.this.localDB(database).versionedStore.shardMarker(version, basename.toInt).exists) {
					availableShards = availableShards + 
						DatabaseVersionShard(database.catalog.name, database.name, version, basename.toInt)
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

		def copy(from: FileSystem#File, to: FileSystem#File) {
			from.readAsReader { reader => 
				to.write(reader, from.size)
			}
		}
	}

	def getBlob(catalog: String, database: String, key: Long): ByteBuffer = {
		val blobDatabase = databases 
			.find { db => db.catalog.name == catalog && db.name == database }
			.getOrElse { throw new DatabaseNotFoundException("Database $database$ not found.") }

		val servedVersion = servingVersions getOrElse (
			blobDatabase, 
			throw new DatabaseNotServedException("Database $blobDatabase.name$ not currently being served.")
		)
		
		val version = servedVersion getOrElse (
			throw new VersionNotFoundException("Version $servedVersion$ not found.")
		)

		val numShards = numShardsPerVersion getOrElse ( 
			(blobDatabase, version),
			throw new ShardsNotFoundException("Shards for database $blobDatabase.name$ version $version$ not found.")
		)

		val shard = DatabaseVersionShard(catalog, database, version, (key % numShards).toInt)

		val nodesServingShard = nodesContent
			.filter { _._2 contains shard }
			.keys
			.toSet

		if (nodesServingShard contains localNode) {
			val reader = new FileStore.Reader {
				val baseFile = localDB(blobDatabase)
					.versionedStore
					.shardMarker(shard.version, shard.shard)
			}

			val blob = reader.get(key)
			reader.close()

			ByteBuffer.wrap(blob)
		} else {
			val node = nodesServingShard.toList.head
			nodeProtocol(node).get(catalog, database, key)
		}
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
		new Catalog(database.catalog.name, fs, rootDir),
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

	def initializeServingVersions() {
		servingVersions = databases
			.map { db => 
				(db, localDB(db).versionedStore.mostRecentVersion) 
			}
			.toMap
	}

	def serveVersion(database: Database, version: Option[Long]) = {
		servingVersions = servingVersions + (database -> version)
	}

	def updateDatabase(database: Database) {
		getNewVersionNumber(database) foreach { version => 
			if (!localDB(database).versionedStore.versionExists(version))
				downloadDatabaseVersion(database, version)
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
}