package chompdb.server

import chompdb.store.VersionedStore
import chompdb.Catalog
import chompdb.Database
import chompdb.store.ShardedWriter
import f1lesystem.FileSystem
import java.util.concurrent.ScheduledExecutorService

abstract class Chomp() {
	val databases: Seq[Database] // List of remote databases in S3?
	val nodes: Map[Node, Endpoint] // TODO: Should Endpoint be a value within Node, or...?
	val nodeProtocolInfo: NodeProtocolInfo
	val nodeAlive: NodeAlive
	val replicationFactor: Int
	val replicationFactorBeforeVersionUpgrade: Int // TODO: Come up with a better name
	val shardIndex: Int // TODO: Clarify this
	val totalShards: Int
	val executor: ScheduledExecutorService
	val fs: FileSystem
	val rootDir: FileSystem#Dir

	@transient var servingVersions: Map[Database, Option[Long]] = Map()

	lazy val nodeProtocol = new NodeProtocol {
		override val chomp = Chomp.this

		def availableShards = nodeProtocolInfo.availableShards(_: Node)
	}

	// TODO: numThreads should not be hard set
	def downloadDatabaseVersion(database: Database, version: Long) = {
		val numThreads = 5

		val remoteDir = database.versionPath(version)

		val localDB = Chomp.this.localDB(database)
		val localDir = localDB.createVersion(version)

		copyShards(remoteDir, localDir)
		copyVersionFile(database.versionMarker(version), localDB.root)

		def copyShards(remoteVersionDir: FileSystem#Dir, localVersionDir: FileSystem#Dir) {
			for (file <- remoteVersionDir.listFiles) {
				copy(file, localVersionDir / file.filename)
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
		.mostRecentVersion
		.flatMap { latestRemoteVersion => 
			localDB(database).mostRecentVersion match {
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

	def initializeServingVersions() = {
		servingVersions = databases
			.map { db => 
				(db, localDB(db).mostRecentVersion) 
			}
			.toMap
	}

	def serveVersion(database: Database, version: Option[Long]) = {
		servingVersions = servingVersions + (database -> version)
	}

	def updateDatabase(database: Database) {
		getNewVersionNumber(database) foreach { version => 
			if (!versionExists(database, version))
				downloadDatabaseVersion(database, version)
		}
	}

	def versionExists(database: Database, version: Long): Boolean = {
		(rootDir /+ database.catalog.name /+ database.name /+ version.toString).exists
	}
}