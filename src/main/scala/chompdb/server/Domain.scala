package chompdb.server

import chompdb._
import f1lesystem.FileSystem

trait DomainManager {
  def apply(fs: FileSystem, root: FileSystem#Path): Option[DomainStore]
  def createLocal(localRoot: String): DomainStore
  def createFromRemote(fs: FileSystem, root: FileSystem#Path): DomainStore
  def create(info: DomainInfo): DomainStore
}

object Domain {
  type Version = Long
  type Shards = Seq[Any]
}

trait Domain {
  import Domain._

  def domainData: Map[Version, ShardSet]
  def domainShards: ShardSet

  /** Returns current version being served. */
  def currentVersion: Version

  /** Returns the latest version. */
  def latestVersion: Version

  def versions: Set[Version]

  def loaded(version: Version): Boolean

  /** Return true if remote versioned store contains newer data than local store. */
  def needsUpdate: Boolean
}