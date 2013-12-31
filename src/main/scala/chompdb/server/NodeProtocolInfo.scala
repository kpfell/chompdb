package chompdb.server

import chompdb.DatabaseVersionShard

abstract class NodeProtocolInfo {
  def availableShards(n: Node): Set[DatabaseVersionShard]
}