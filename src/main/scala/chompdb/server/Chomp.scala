package chompdb.server

import chompdb.store.VersionedStore
import chompdb.Catalog
import chompdb.Database
import chompdb.store.ShardedWriter
import f1lesystem.FileSystem
import java.util.concurrent.ScheduledExecutorService

abstract class Chomp() {
	val databases: Seq[Database]
	val nodes: Map[Node, Endpoint] 
	val nodeProtocolInfo: NodeProtocolInfo
	val nodeAlive: NodeAlive
	val replicationFactor: Int
	val replicationBeforeVersionUpgrade: Int // TODO: Come up with a better name
	val shardIndex: Int
	val totalShards: Int
	val executor: ScheduledExecutorService
	val fs: FileSystem
	val rootDir: FileSystem#Dir

	@transient var servingVersions: Map[Database, Option[Long]] = Map()
	@transient var nodesServingVersions: Map[Node, Map[Database, Option[Long]]] = Map()

	lazy val nodeProtocol = new NodeProtocol {
		override val chomp = Chomp.this

		def allAvailableShards = nodeProtocolInfo.allAvailableShards(_: Node)
		def availableShards = nodeProtocolInfo.availableShards(_: Node, _: Database)
		def availableShardsForVersion = nodeProtocolInfo.availableShardsForVersion(_: Node, _: Database, _: Long)
		def availableVersions = nodeProtocolInfo.availableVersions(_: Node, _: Database)
		def mostRecentRemoteVersion = nodeProtocolInfo.latestVersion(_: Node, _: Database)
		def serveVersion = nodeProtocolInfo.serveVersion(_: Node, _: Database, _: Option[Long])
		def retrieveVersionsServed = nodeProtocolInfo.retrieveVersionsServed(_: Node)
	}

	// TODO: numThreads should not be hard set
	def downloadDatabaseVersion(database: Database, version: Long) = {
		val numThreads = 5

		val remoteDir = database.versionPath(version)

		// TODO: This "fails" silently if the version does not exist.
		if (database.versionMarker(version).exists) {
			val localDB = Chomp.this.localDB(database)
			val localDir = localDB.createVersion(version)

			// TODO: This causes problems with NodeProtocol.serveVersion, for example, if
			// the version fails to transfer completely
			copyShards(remoteDir, localDir)
			copyVersionFile(database.versionMarker(version), localDB.root)
		}

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
			if (!localDB(database).versionExists(version))
				downloadDatabaseVersion(database, version)
		}
	}
}