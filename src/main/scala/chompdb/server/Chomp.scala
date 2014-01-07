package chompdb.server

import chompdb._
import chompdb.store.VersionedStore
import chompdb.store.ShardedWriter
import f1lesystem.FileSystem
import java.util.concurrent.ScheduledExecutorService
import scala.collection._

// object Chomp {
// 	class LocalNodeProtocol(node: Node, chomp: Chomp) extends NodeProtocol {
// 		def availableShards(database: Database): Set[DatabaseVersionShard] = {
// 			chomp.availableShards filter(_.database == database)
// 		}
// 	}
// }

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

	@transient private[server] var availableShards = Set.empty[DatabaseVersionShard]

	@transient var servingVersions/*: Map[Database, Option[Long]]*/ = Map.empty[Database, Option[Long]]
	@transient var nodesServingVersions = Map.empty[Node, Map[Database, Option[Long]]]

	@transient var nodesAlive = Map.empty[Node, Boolean]
	@transient var nodesContent = Map.empty[Node, Set[DatabaseVersionShard]]

	lazy val nodeProtocol = new NodeProtocol {
		override val chomp = Chomp.this

		def availableShards = nodeProtocolInfo.availableShards(_: Node, _: Database)
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

	def updateNodesAlive() {
		nodesAlive = nodes
			.keys
			.map( n => n -> nodeAlive.isAlive(n) )(breakOut)
	}

	def updateNodesContent() {
		nodesContent = nodes
			.keys
			.map { n => n -> databases
				.map { db => nodeProtocol.availableShards(n, db) }
				.flatten
				.toSet
			}
			.toMap

	}
}