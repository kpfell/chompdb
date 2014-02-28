package chompdb.server

import java.util.Collections.synchronizedSortedMap
import java.util.TreeMap
import scala.collection._

case class InvalidReplicationFactorException(smth: String) extends Exception
case class NodeNotFoundException(smth: String) extends Exception

class HashRing(chomp: Chomp) {
  val nodeMap = new TreeMap[Int, Node]

  def addNode(node: Node) {
    chomp.log.debug(s"Adding $node to hashRing for $chomp on node $chomp.localNode")
    val nodeHashValue = Hashing.hash(node.id)
    nodeMap put (nodeHashValue, node)
  }

  def getNodesForShard(shardId: Int): Set[Node] = {
    chomp.log.debug(s"Getting nodes for shard $shardId on $chomp.localNode $chomp")

    val shardHashValue = Hashing.hash(shardId.toString)

    nodeMap.size match {
      case 0 => {
        chomp.log.error(
          s"No nodes available on hashRing for $chomp.localNode $chomp",
          throw new NodeNotFoundException(s"No nodes available on hashRing for $chomp.localNode $chomp")
        )
        throw new NodeNotFoundException(s"No nodes available on hashRing for $chomp.localNode $chomp")
      }
      case 1 => Set(nodeMap.get(nodeMap.firstKey))
      case _ => {
        def assembleNodeSet(nodeSet: Set[Node], count: Int, baseHashValue: Int): Set[Node] = count match {
          case c if c < 0 => {
            chomp.log.error(
              s"Invalid replicationFactor provided for $chomp.localNode $chomp : $c",
              throw new InvalidReplicationFactorException(s"Invalid replicationFactor provided for $chomp.localNode $chomp : $c")
            )
            throw new InvalidReplicationFactorException(s"Invalid replicationFactor provided for $chomp.localNode $chomp : $c")
          }
          case 0 => nodeSet
          case c if c > 0 => {
            val nearestNodeHashValue = Option(nodeMap.ceilingKey(baseHashValue))
              .getOrElse(nodeMap.firstKey)

            assembleNodeSet(nodeSet + nodeMap.get(nearestNodeHashValue), c - 1, nearestNodeHashValue + 1)
          }
        }

        assembleNodeSet(Set.empty[Node], chomp.replicationFactor, shardHashValue)
      }
    } 
  }

  def initialize(nodes: Set[Node]) {
    chomp.log.debug(s"Initializing hashRing for $chomp.localNode $chomp")
    nodes foreach { n =>
      val nodeHashValue = Hashing.hash(n.id)
      nodeMap put (nodeHashValue, n)
    }
  }
}