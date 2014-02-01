package chompdb.server

import scala.collection.immutable.TreeMap

case class NodeNotFoundException(smth: String) extends Exception

trait HashRing {
  @transient var nodeMap = TreeMap.empty[Int, Node]

  def addNode(node: Node) = {
    val nodeHashValue = Hashing.hash(node.id)
    nodeMap = nodeMap + (nodeHashValue -> node)
  }

  def getNextNode(key: Long): Node = {
    val keyHashValue = Hashing.hash(key.toString)

    nodeMap.size match {
      case 0 => throw new NodeNotFoundException("No nodes available on the hash ring.")
      case 1 => nodeMap.head._2
      case _ => {
        nodeMap 
          .find { nhv => nhv._1 >= keyHashValue } 
          .get._2
        }
    } 
  }
}