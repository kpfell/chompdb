package chompdb.server

import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemRequest

import java.util.Arrays
import java.util.concurrent.TimeUnit

import pixii._
import pixii.AttributeModifiers._
import pixii.AttributeValueConversions._

import scala.collection._
import scala.collection.JavaConversions._
import scala.concurrent.duration._

case class NodeTimestamp(node: Node, ts: Long)

trait NodeAlive {
	def isAlive(node: Node): Boolean
	def imAlive(): Unit
}

abstract class AliveTable extends Table[NodeTimestamp] { //with TableOperations[String, NodeTimestamp] {
	val nodeId = new NamedAttribute[String]("nodeId") with Required
	val ts = new NamedAttribute[String]("ts") with Required

	val hashKeyAttribute = nodeId

	override val itemMapper = new ItemMapper[NodeTimestamp] {
		override def apply(value: NodeTimestamp): Map[String, AttributeValue] = {
			nodeId.apply(value.node.id) ++ ts.apply(value.ts.toString)
		}

		override def unapply(attributes: Map[String, AttributeValue]): NodeTimestamp = {
			NodeTimestamp(Node(nodeId.get(attributes)), ts.get(attributes).toLong)
		}
	}	
}

// TODO: In sketch, retryPolicy included maxRetries
abstract class DynamoNodeAlive extends NodeAlive {
	class SimpleLogger extends ConsoleLogger

	val gracePeriod: Duration
	val tableName: String
	protected val dynamoDB: AmazonDynamoDB
	protected val retryPolicy = new FibonacciBackoff((100, MILLISECONDS), new SimpleLogger()) // Need a Logger to create Fibonacci backoff
	val table = new AliveTable {
		override val dynamoDB = DynamoNodeAlive.this.dynamoDB
		override val retryPolicy = DynamoNodeAlive.this.retryPolicy
		override val tableName = DynamoNodeAlive.this.tableName
	}

	// TODO: Abstract away the DynamoDB request with pixii
	def isAlive(node: Node): Boolean = {
		val key = Map("nodeId" -> new AttributeValue().withN(node.id))

		val getItemRequest = new GetItemRequest()
			.withTableName(tableName)
			.withKey(key)
			.withAttributesToGet(Arrays.asList("ts"))

		val result = dynamoDB.getItem(getItemRequest)

		val latestTs = result
			.getItem
			.get("nodeId")
			.getS
			.toLong

		if (Duration((System.currentTimeMillis() - latestTs), MILLISECONDS) < gracePeriod) true
		else false
	}
}

// // // DynamoNodeAlive user
// // val nodeAlive = new DynamoNodeAlive {
// // 	val tableName = ...
// // 	val dynamoDB = ...
// // }

// // nodeAlive.table.create(readCapacity = 10, writeCapacity = 4)