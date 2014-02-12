package chompdb.server

import java.util.TreeMap

case class NodeNotFoundException(smth: String) extends Exception

class HashRing {
  val nodeMap = new TreeMap[Int, Node]

  def addNode(node: Node) = {
    val nodeHashValue = Hashing.hash(node.id)
    nodeMap put (nodeHashValue, node)
  }

  def getNodeForShard(shardId: Int): Node = {
    val shardHashValue = Hashing.hash(shardId.toString)

    nodeMap.size match {
      case 0 => throw new NodeNotFoundException("No nodes available on the hash ring.")
      case 1 => nodeMap.get(nodeMap.firstKey)
      case _ => {
        val nearestNodeHashValue = Option(nodeMap.ceilingKey(shardHashValue))
          .getOrElse(nodeMap.firstKey)
        nodeMap.get(nearestNodeHashValue)
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