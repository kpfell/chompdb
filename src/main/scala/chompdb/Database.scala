package chompdb

import java.util.Properties
import scala.collection.JavaConverters._
import f1lesystem.FileSystem
import chompdb.store.VersionedStore

class Database(
  val catalog: Catalog,
  val name: String
) {
  val versionedStore = new VersionedStore {
    override val fs = catalog.fs
    override val root = (catalog.dir /+ name).asInstanceOf[fs.Dir] // TODO: Remove cast
  }

  override def equals(other: Any) = other match {
    case d: Database => (d.catalog == catalog) && (d.name == name)
  }

  def shardsOfVersion(version: Long): Set[DatabaseVersionShard] = versionedStore
    .shardNumsOfVersion(version)
    .map { shardNum => DatabaseVersionShard(catalog.name, name, version, shardNum) }

}