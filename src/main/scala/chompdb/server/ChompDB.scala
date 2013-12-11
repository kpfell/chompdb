package chompdb.server

import chompdb.Database
import f1lesystem.FileSystem

class ChompDB(
	val databases: Seq[Database],
	val replicationFactor: Int,
	val replicationFactorBeforeVersionUpgrade: Int,
	val shardIndex: Int,
	val totalShards: Int,
	val executor: ScheduledExecutor,
	val fs: FileSystem,
	val rootDir: FileSystem#Dir
) {
	// def downloadDatabaseVersion(version: Int)

	// def start() {
	// 	executor.schedule(timerTask, period)
	// }
}