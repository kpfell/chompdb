package chompdb.sharding

trait Sharded {
  val shardsIndex: Int
  val shardsTotal: Int
  
  def split(index: Int, splits: Int) = new Sharded {
    val shardsIndex = (Sharded.this.shardsIndex * splits) + index
    val shardsTotal = (Sharded.this.shardsTotal * splits)
    override def toString = s"SplitSharded(shardsIndex=$shardsIndex, shardsTotal=$shardsTotal"
  }
}
