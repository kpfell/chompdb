package chompdb.server

import chompdb.store.VersionedStore
import chompdb.Database
import chompdb.store.ShardedWriter
import f1lesystem.FileSystem
import java.util.concurrent.ScheduledExecutorService

abstract class ChompDB() {

	val databases: Seq[Database]
	val replicationFactor: Int
	val replicationFactorBeforeVersionUpgrade: Int
	val shardIndex: Int
	val totalShards: Int
	val executor: ScheduledExecutorService
	val fs: FileSystem
	val rootDir: FileSystem#Dir

	/* Returns version number, if any, of the latest database version to download from S3. */
	def getNewVersionNumber(database: Database): Option[Long] = {
		database.versionedStore.mostRecentVersion flatMap { latestRemoteVersion => 
			localVersionedStore(database).mostRecentVersion match {
				case Some(latestLocalVersion) =>
					if (latestRemoteVersion > latestLocalVersion) Some(latestRemoteVersion)
					else None
				case None => Some(latestRemoteVersion)
			}
		}
	}

	// TODO: numThreads should not be hard set
	def downloadDatabaseVersion(database: Database, version: Long) = {
		val numThreads = 5

		val remoteDir = database.versionedStore.versionPath(version)

		val localVersionedStore = ChompDB.this.localVersionedStore(database)
		val localDir = localVersionedStore.createVersion(version)

		copyShards(remoteDir, localDir)
		copyVersionFile(database.versionedStore.versionMarker(version), localVersionedStore.root)

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

	def localVersionedStore(database: Database): VersionedStore = new VersionedStore {
		override val fs = ChompDB.this.fs
		override val root = (rootDir /+ database.catalog.name /+ database.name).asInstanceOf[fs.Dir] // TODO: remove cast
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

	// def start() {
	// 	executor.schedule(timerTask, period)
	// }
}