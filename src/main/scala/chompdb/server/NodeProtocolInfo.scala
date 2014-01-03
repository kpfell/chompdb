package chompdb.server

import chompdb.Database
import chompdb.DatabaseVersionShard

abstract class NodeProtocolInfo {
  def allAvailableShards(n: Node): Set[DatabaseVersionShard]
  def availableShards(n: Node, db: Database): Set[DatabaseVersionShard]
  def availableShardsForVersion(n: Node, db: Database, v: Long): Set[DatabaseVersionShard]
  def availableVersions(n: Node, db: Database): Set[Option[Long]]
  def latestVersion(n: Node, db: Database): Option[Long]
  def serveVersion(n: Node, db: Database, v: Option[Long]): Boolean
  def retrieveVersionsServed(n: Node): Map[Database, Option[Long]]
}