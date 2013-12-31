package chompdb

import java.util.Properties
import scala.collection.JavaConverters._
import f1lesystem.FileSystem
import chompdb.store.VersionedStore

class Database(
  val catalog: Catalog,
  val name: String
) extends VersionedStore {
  override val fs = catalog.fs
  override val root = (catalog.dir /+ name).asInstanceOf[fs.Dir] // TODO: Remove cast

  override def equals(other: Any) = other match {
    case d: Database => (d.catalog == catalog) && (d.name == name)
  }

  /* Primarily for testing purposes */
  def createEmptyShard(version: Long) {
    val filename = lastShardNum(version) match {
      case None => 0.toString
      case Some(s) => (s + 1).toString
    }

    val blobFile = root /+ version.toString / (filename + ".blob")
    val indexFile = root /+ version.toString / (filename + ".index")

    blobFile.touch()
    indexFile.touch()
  }

  def lastShardNum(version: Long): Option[Int] = {
    versionPath(version)
      .listFiles
      .filter(_.extension == "blob")
      .filter(_.basename forall Character.isDigit)
      .map { f => f.basename.toInt }
      .reduceLeftOption(_ max _)
  }

  def retrieveShards(version: Long): Set[DatabaseVersionShard] = versionPath(version)
    .listFiles
    .filter(_.extension == "blob")
    .map { blobFile =>
      DatabaseVersionShard(
        catalog.name,
        name,
        version,
        blobFile.basename.toInt
      )
    }
    .toSet
}