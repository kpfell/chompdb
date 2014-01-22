package chompdb.dynamodb

import chompdb.server.Node
import chompdb.server.NodeAlive

import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemRequest

import java.util.Arrays

import pixii._
import pixii.AttributeModifiers._
import pixii.AttributeValueConversions._

import scala.collection._
import scala.collection.JavaConverters._
import scala.concurrent.duration._

object DynamoNodeAlive {
  case class NodeTimestamp(node: Node, ts: Long)

  val nodeId = new NamedAttribute[String]("nodeId") with Required
  val ts = new NamedAttribute[String]("ts") with Required
  
  trait AliveTable extends Table[NodeTimestamp] with HashKeyTable[String, NodeTimestamp] {
    override val hashKeyAttribute = nodeId
  
    override val itemMapper = new ItemMapper[NodeTimestamp] {
      override def apply(value: NodeTimestamp): Map[String, AttributeValue] = {
        nodeId.apply(value.node.id) ++ ts.apply(value.ts.toString)
      }
  
      override def unapply(attributes: Map[String, AttributeValue]): NodeTimestamp = {
        NodeTimestamp(Node(nodeId.get(attributes)), ts.get(attributes).toLong)
      }
    } 
  }
}

import DynamoNodeAlive._

trait DynamoNodeAlive extends NodeAlive {
  val logger: Logger = new ConsoleLogger {}

  val tableName: String
  
  val gracePeriod: Duration
  
  protected val dynamoDB: AmazonDynamoDB
  
  protected val retryPolicy = new FibonacciBackoff((10, SECONDS), logger)
  
  val table = new AliveTable {
    override val dynamoDB = DynamoNodeAlive.this.dynamoDB
    override val retryPolicy = DynamoNodeAlive.this.retryPolicy
    override val tableName = DynamoNodeAlive.this.tableName
  }

  def isAlive(node: Node): Boolean = {
    val nodeTimestamp = table(node.id)(ConsistentRead) getOrElse { sys.error(s"Unknown node: $node") }
    (System.currentTimeMillis - nodeTimestamp.ts).millis < gracePeriod
  }
}
