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
    override val root: FileSystem#Dir = catalog.dir /+ name
  }

  override def equals(other: Any) = other match {
    case d: Database => (d.catalog == catalog) && (d.name == name)
  }

  def shardsOfVersion(version: Long): Set[DatabaseVersionShard] = versionedStore
    .shardNumsForVersion(version)
    .map { shardNum => DatabaseVersionShard(catalog.name, name, version, shardNum) }

  override def toString = s"Database($name; $catalog)"
}