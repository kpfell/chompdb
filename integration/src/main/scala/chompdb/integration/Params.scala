package chompdb.integration

import chompdb._
import f1lesystem._

case class Params(
  databases: Seq[(Database, DatabaseParams)]
) {

  lazy val catalogs: Set[Catalog] = databases map (_._1.catalog) toSet
}

case class DatabaseParams(
  /** Number of key-value pairs to generate per database */
  val nElements: Long,

  val shardsTotal: Int,

  val blobSizeRange: (Int, Int)
)

