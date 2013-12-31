package chompdb.server

import chompdb.Database
import chompdb.DatabaseVersionShard

abstract class NodeProtocolInfo {
  def allAvailableShards(n: Node): Set[DatabaseVersionShard]
  def availableShards(n: Node, db: Database): Set[DatabaseVersionShard]
}