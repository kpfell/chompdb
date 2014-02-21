package chompdb

import java.util.Properties
import scala.collection.JavaConverters._

object DatabaseVersion{
  def fromProperties(version: Long, props: Properties) = {
    DatabaseVersion(
      version,
      props.getProperty("sharding_factor").toInt,
      props.getProperty("splits").toInt,
      props.asScala.toMap
    )
  }
}

case class DatabaseVersion(
  version: Long,
  shardingFactor: Int,
  splits: Int,
  properties: Map[String, String]
) {
  def toProperties = {
    val props = new Properties()
    props.put("sharding_factor", shardingFactor.toString)
    props.put("splits", splits.toString)
  }
}
