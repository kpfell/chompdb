package chompdb.integration

object Blob {
  import java.io._

  case class Data(
    database: String,
    version: Long,
    writer: Int,
    index: Int,
    size: Int
  )
  
  private val random = new RandomUtils(); import random._
  
  /** Generate a blob of `size` including encoded including database, version and arbitrary `n` value */ 
  def apply(data: Data): Array[Byte] = {
    import data._
    val baos = new ByteArrayOutputStream(size + 1024)
    val out = new DataOutputStream(baos)
    out.writeUTF(database)
    out.writeLong(version)
    out.writeInt(writer)
    out.writeInt(index)
    out.writeInt(size)
    out.write(filler(size), 0, size)
    baos.toByteArray()
  }
  
  def unapply(buf: Array[Byte]) = {
    val bais = new ByteArrayInputStream(buf)
    val in = new DataInputStream(bais)
    
    val database = in.readUTF()
    val version  = in.readLong()
    val writer   = in.readInt()
    val index    = in.readInt()
    val size     = in.readInt()
    
    // TODO: validate filler
    
    Data(database, version, writer, index, size)
  }

  private def filler(size: Int): Array[Byte] = {
    val buf = new Array[Byte](size)
    var i = 0
    while (i < size) {
      buf(i) = i.toByte
      i += 1
    }
    buf
  }
}