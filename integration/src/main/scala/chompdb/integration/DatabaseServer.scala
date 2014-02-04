package chompdb.integration

import chompdb.server._
import scala.concurrent.duration._

trait DatabaseServer extends Chomp { 
  
  val params: Params

  override val nodesServingVersionsFreq = 1.minute
  override val nodesAliveFreq = 1.minute
  override val nodesContentFreq = 1.minute    
  
  override def serializeMapReduce[T, U](mapReduce: MapReduce[T, U]): String = "mapReduce"

}