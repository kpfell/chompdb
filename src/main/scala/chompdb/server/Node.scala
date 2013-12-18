package chompdb.server

case class Node(id: String)

// SWITCH TO TRAIT WHEN IMPLEMENTING
class NodeProtocol {
// 	def available: Set[DatabaseVersionShard]
// 	def mapReduce(dv: DatabaseVersion, ids: Set[Long])(map: T => U)(reduce: (U, U) => U): U
}

// new AkkaProtocol(node.endpoint) {
// 	  // endpoint = "ec2-##-###-###-###.compute-1.amazonaws.com:8282"
// }