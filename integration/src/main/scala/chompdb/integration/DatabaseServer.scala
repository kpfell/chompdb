package chompdb.integration

import chompdb.server._
import scala.concurrent.duration._
import scala.reflect.runtime.universe._
import java.nio.ByteBuffer

trait DatabaseServer extends Chomp {

  val params: Params

  override val databaseUpdateFreq  = 1.minute
  override val nodesAliveFreq      = 1.minute
  override val nodesContentFreq    = 1.minute
  override val servingVersionsFreq = 1.minute

  override def serializeMapReduce[T, U](mapReduce: MapReduce[T, U]): String = "mapReduce"

  override def deserializeMapReduce(mapReduce: String): MapReduce[ByteBuffer, _] = {
    ???
  }

  override def serializeMapReduceResult(result: Any): Array[Byte] = {
    ???
  }

  override def deserializeMapReduceResult[T: TypeTag](result: Array[Byte]): T = {
    ???
  }

}