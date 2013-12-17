package chompdb.server

trait NodeAlive {
// 	def isAlive(node: Node): Boolean

// 	def imAlive(): Unit
}

object DynamoNodeAlive {
// 	case class NodeTimestamp {
// 		node: Node
// 		ts: Long
	}

	// private trait AliveTable extends Table[NodeTimestamp] {
// 		val nodeId = new NamedAttribute[String]("nodeId") with Required
// 		val ts = new NamedAttribute[String]("ts") with Required

// 		override val hashKeyAttribute = nodeId

// 		override val itemMapper = new ItemMapper[NodeTimestamp] {
// 			override def apply(value: NodeTimestamp): Map[String, AttributeValue] = {
// 				nodeId.apply(value.nodeId) ++
// 				ts.apply(value.ts)
// 			}

// 			override def unapply(attributes: Map[String, AttributeValue]): NodeTimestamp = {
// 				NodeTimestamp(
// 					nodeId.get(attributes),
// 					ts.get(attributes),
// 				)
// 			}
// 		}
// 	}
// }

class DynamoNodeAlive extends NodeAlive {
// 	val gradePeriod: Duration
// 	val tableName: String
// 	protected val dynamoDB: AmazonDynamoDB
// 	protected val retryPolicy = FibonacciBackoffPolicy(maxRetries = 10, startMillis = 100 millis)

// 	val table = new AliveTable {
// 		override val dynamoDB = DynamoNodeAlive.this.dynamoDB
// 		override val retryPolicy = DynamoNodeAlive.this.retryPolicy
// 		override val tableName = DynamoNodeAlive.this.tableName
// 	}
}

// // // DynamoNodeAlive user
// // val nodeAlive = new DynamoNodeAlive {
// // 	val tableName = ...
// // 	val dynamoDB = ...
// // }

// // nodeAlive.table.create(readCapacity = 10, writeCapacity = 4)