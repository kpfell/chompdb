package chompdb

import java.util.Properties
import scala.collection.JavaConverters._

object Chomp {
  def deserialize(name: String, content: String) = {
    Chomp(
      name, 
      versions = content.split(',') map (_.toLong) toSet
    )
  }
}

case class Chomp(
  name: String,
  versions: Set[Long]
) {
  def serialize = (versions mkString ",")
}
