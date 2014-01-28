package chompdb.integration

import chompdb.server._

trait DatabaseServer extends Chomp { 
  
  val params: Params

  override def serializeMapReduce[T, U](mapReduce: MapReduce[T, U]): String = "mapReduce"

}