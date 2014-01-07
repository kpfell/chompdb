package chompdb.server

import chompdb.Database
import chompdb.DatabaseVersionShard

abstract class NodeProtocolInfo {
  def availableShards(n: Node, db: Database): Set[DatabaseVersionShard]
  def serveVersion(n: Node, db: Database, v: Option[Long]): Boolean
  def retrieveVersionsServed(n: Node): Map[Database, Option[Long]]
}