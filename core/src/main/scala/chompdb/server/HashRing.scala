package chompdb.server

import java.util.Collections.synchronizedSortedMap
import java.util.TreeMap

case class InvalidReplicationFactorException(smth: String) extends Exception
case class NodeNotFoundException(smth: String) extends Exception

class HashRing(chomp: Chomp) {
  val nodeMap = new TreeMap[Int, Node]

  def addNode(node: Node) {
    val nodeHashValue = Hashing.hash(node.id)
    nodeMap put (nodeHashValue, node)
  }

  def getNodesForShard(shardId: Int): Set[Node] = {
    val shardHashValue = Hashing.hash(shardId.toString)

    nodeMap.size match {
      case 0 => throw new NodeNotFoundException("No nodes available on the hash ring.")
      case 1 => Set(nodeMap.get(nodeMap.firstKey))
      case _ => {
        def assembleNodeSet(nodeSet: Set[Node], count: Int, baseHashValue: Int): Set[Node] = count match {
          case c if c < 0 => {
            throw new InvalidReplicationFactorException(s"Invalid replicationFactor provided: $c")
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
    nodes foreach { n =>
      val nodeHashValue = Hashing.hash(n.id)
      nodeMap put (nodeHashValue, n)
    }
  }
}