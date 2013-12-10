package chompdb

import java.util.Properties
import scala.collection.JavaConverters._
import f1lesystem.FileSystem
import chompdb.store.VersionedStore

class Database(
  val catalog: Catalog,
  val name: String
) {
  private[chompdb] lazy val versionedStore = new VersionedStore {
    override val fs = catalog.fs
    override val root = (catalog.dir /+ name).asInstanceOf[fs.Dir] // TODO: remove cast
  }

  def createVersion(version: Long) = versionedStore.createVersion(version)

  def succeedVersion(version: Long) = versionedStore.succeedVersion(version)
  
  override def equals(other: Any) = other match {
    case d: Database => (d.catalog == catalog) && (d.name == name)
  }
}