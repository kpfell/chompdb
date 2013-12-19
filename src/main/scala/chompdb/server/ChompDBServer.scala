package chompdb.server

import chompdb.DatabaseVersionShard
import scala.collection._

abstract class ChompDBServer {
	val chompDB: ChompDB

	@transient var nodesAlive: Map[Node, Boolean] = Map()
	@transient var nodesContent: Map[Node, Set[DatabaseVersionShard]] = Map()

	def assignShards(shards: Set[DatabaseVersionShard]) = {
		// Empty for now
	}

	def updateNodesAlive() = {
		nodesAlive = chompDB
			.nodes
			.keys
			.map({ n => n -> chompDB.nodeAlive.isAlive(n) })(breakOut)
	}

	def updateNodesContent() = {
		nodesContent = chompDB
			.nodes
			.keys
			.map({ n => n -> chompDB.nodeProtocol.available(n) })(breakOut)
	}
}