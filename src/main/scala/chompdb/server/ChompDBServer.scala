package chompdb.server

import chompdb.DatabaseVersionShard
import scala.collection._

abstract class ChompDBServer {
	val chompDB: ChompDB

	@transient var nodesAlive: Map[Node, Boolean] = Map()
	@transient var nodesContent: Map[Node, Set[DatabaseVersionShard]] = Map()

	def assignShards(shards: Set[DatabaseVersionShard]): Map[Node, Set[DatabaseVersionShard]] = {
		// val replicationFactor = chompDB.replicationFactor
		val nodesList = chompDB.nodes.keys.toList

		// Does not account for replication factor
		shards
			.zipWithIndex
			.map{ case (shard, idx) => (nodesList(idx % nodesList.length), shard)}
			.groupBy(_._1)
			.map{ case (k, v) => (k, v.map(_._2)) }
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