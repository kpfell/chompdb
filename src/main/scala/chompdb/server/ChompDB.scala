package chompdb.server

import chompdb.Database
import f1lesystem.FileSystem

// TODO: Add root directory parameter in a way that doesn't result in a type error
class ChompDB(
	val databases: Seq[Database],
	val replicationFactor: Int,
	val replicationFactorBeforeVersionUpgrade: Int,
	val shardIndex: Int,
	val totalShards: Int,
	val executor: ScheduledExecutor,
	val fs: FileSystem
	// val rootDir: fs.Dir
) {
	// def downloadDatabaseVersion(version: Int)

	// def start() {
	// 	executor.schedule(timerTask, period)
	// }
}