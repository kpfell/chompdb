package chompdb

import java.nio.ByteBuffer

trait Store {
}

object KV {
  trait Reader {
    def get(key: Long): Option[ByteBuffer]
  }
  
  trait Writer {
    def put(value: ByteBuffer): Long
  }
}

import java.util.concurrent.atomic.AtomicLong

object FileSystemWriter {
}

trait FileSystemWriter extends KV.Writer {

  val fs: FileSystem
  
  val root: fs.Dir

  val shardsIndex: Int

  val shardsTotal: Int
  
  def lastId = nextId.get() - 1

  private[chompdb] final lazy val nextId = new AtomicLong(shardsIndex)
  
  override def put(value: ByteBuffer): Long = {
    val id = nextId.getAndAdd(shardsTotal)
    val file = fileForId(id)
    file.parent.mkdir()
    file.write(value)
    id
  }
  
  private[chompdb] def fileForId(id: Long): fs.File = {
    val hex = java.lang.Long.toHexString(id)
    val padded = if (hex.length % 2 == 1) '0' + hex else hex
    val groups =  (padded grouped 2).toSeq
    val subDirs = groups dropRight 1
    val filename = groups.last
    val dir = (root /: subDirs) { (parent, subDir) => parent /+ ('D' + subDir) }
    dir / filename	
  }
}
