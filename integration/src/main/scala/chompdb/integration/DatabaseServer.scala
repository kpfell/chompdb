package chompdb.integration

import chompdb.server._
import scala.concurrent.duration._
import scala.reflect.runtime.universe._
import java.nio.ByteBuffer
import java.io._

object DatabaseServer {
  def serialize(result: Any): Array[Byte] = {
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(result)
    o.flush()
    o.close()
    b.toByteArray()
  }

  def deserialize[T](result: Array[Byte]): T = {
    val b = new ByteArrayInputStream(result)
    val o = new ObjectInputStream(b)
    o.readObject().asInstanceOf[T]
  }
}
trait DatabaseServer extends Chomp {
  import DatabaseServer._

  val params: Params

  override val databaseUpdateFreq  = 1.minute
  override val nodesAliveFreq      = 1.minute
  override val nodesContentFreq    = 1.minute
  override val servingVersionsFreq = 1.minute

  override def serializeMapReduce[T, U](mapReduce: MapReduce[T, U]): String = "mapReduce"

  override def deserializeMapReduce(mapReduce: String): MapReduce[ByteBuffer, _] = {
    mapReduce match {
      case "mapReduce" => DatabaseClient.CollectVersions 
    }
  }

  override def serializeMapReduceResult(result: Any): Array[Byte] = {
    serialize(result)
  }

  override def deserializeMapReduceResult[T: TypeTag](result: Array[Byte]): T = {
    deserialize(result)
  }

}