package chompdb.server

import chompdb._
import chompdb.store.VersionedStore
import chompdb.store.ShardedWriter
import f1lesystem.FileSystem
import java.util.concurrent.ScheduledExecutorService
import scala.collection._

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
	}
}

abstract class Chomp() {
	val databases: Seq[Database]
	val nodes: Map[Node, Endpoint] 
	val nodeAlive: NodeAlive
	val replicationFactor: Int
	val replicationBeforeVersionUpgrade: Int // TODO: Come up with a better name
	val shardIndex: Int
	val totalShards: Int
	val executor: ScheduledExecutorService
	val fs: FileSystem
	val rootDir: FileSystem#Dir

	@transient private[server] var availableShards = Set.empty[DatabaseVersionShard]

	@transient var servingVersions/*: Map[Database, Option[Long]]*/ = Map.empty[Database, Option[Long]]
	@transient var nodesServingVersions = Map.empty[Node, Map[Database, Option[Long]]]

	@transient var nodesAlive = Map.empty[Node, Boolean]
	@transient var nodesContent = Map.empty[Node, Set[DatabaseVersionShard]]

	def nodeProtocol: Map[Node, NodeProtocol]

	// TODO: numThreads should not be hard set
	def downloadDatabaseVersion(database: Database, version: Long) = {
		val numThreads = 5

		val remoteDir = database.versionedStore.versionPath(version)

		// TODO: This "fails" silently if the version does not exist.
		if (database.versionedStore.versionMarker(version).exists) {
			val localDB = Chomp.this.localDB(database)
			val localDir = localDB.versionedStore.createVersion(version)

			// TODO: This causes problems with NodeProtocol.serveVersion, for example, if
			// the version fails to transfer completely
			copyShards(remoteDir, localDir)
			copyVersionFile(database.versionedStore.versionMarker(version), localDB.versionedStore.root)
		}

		def copyShards(remoteVersionDir: FileSystem#Dir, localVersionDir: FileSystem#Dir) {
			val remoteBasenames = remoteVersionDir
				.listFiles
				.map { _.basename }
				.filter { _ forall Character.isDigit }
				.toSet

			for (basename <- remoteBasenames) {
				val blobFile = remoteVersionDir / (basename + ".blob")
				copy(blobFile, localVersionDir / blobFile.filename)

				val indexFile = remoteVersionDir / (basename + ".index")
				copy(indexFile, localVersionDir / indexFile.filename)

				if ((localVersionDir / blobFile.filename).exists && (localVersionDir / indexFile.filename).exists) {
					database.versionedStore.succeedShard(version, basename.toInt)
				}
			}
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
			.map { db => db.versionedStore.versions 
				.map { v => db.shardsOfVersion(v) } 
			}
			.toSet
			.flatten
			.flatten
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