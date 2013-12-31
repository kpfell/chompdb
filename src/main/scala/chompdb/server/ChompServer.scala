package chompdb.server

import chompdb.DatabaseVersionShard
import scala.collection._

abstract class ChompServer {
  val chomp: Chomp

  @transient var nodesAlive: Map[Node, Boolean] = Map()
  @transient var nodesContent: Map[Node, Set[DatabaseVersionShard]] = Map()

  def updateNodesAlive() = {
    nodesAlive = chomp
      .nodes
      .keys
      .map( n => n -> chomp.nodeAlive.isAlive(n) )(breakOut)
  }

  def updateNodesContent() = {
    nodesContent = chomp
      .nodes
      .keys
      .map( n => n -> chomp.nodeProtocol.allAvailableShards(n) )(breakOut)
  }
}