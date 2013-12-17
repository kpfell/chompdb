package chompdb.server

trait Node {
// 	val id: String
// 	val endpoint: String
}

trait NodeProtocol {
// 	def available: Set[DatabaseVersionShard]
// 	def mapReduce(dv: DatabaseVersion, ids: Set[Long])(map: T => U)(reduce: (U, U) => U): U
}

// new AkkaProtocol(node.endpoint) {
// 	  // endpoint = "ec2-##-###-###-###.compute-1.amazonaws.com:8282"
// }