package chompdb

import java.util.Properties
import scala.collection.JavaConverters._

object DatabaseInfo {
  def deserialize(name: String, content: String) = {
    DatabaseInfo(
      name,
      versions = content.split(',') map (_.toLong) toSet
    )
  }
}

case class DatabaseInfo(
  name: String,
  versions: Set[Long]
) {
  def serialize = (versions mkString ",")
}
