package chompdb.server

trait NodeAlive {
  def isAlive(node: Node): Boolean
  def imAlive(): Unit
}
