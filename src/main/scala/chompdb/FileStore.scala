package chompdb

import f1lesystem.FileSystem
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import scala.collection._
import java.io.EOFException

object FileStore {

  private[FileStore] trait BaseFile {
    val baseFile: FileSystem#File
    
    lazy val blobFile  = baseFile.parent / s"${baseFile.filename}.blob"
    lazy val indexFile = baseFile.parent / s"${baseFile.filename}.index"
  }
  
  trait Reader extends Store.Reader with BaseFile {
    import java.io.{RandomAccessFile, DataInputStream, BufferedInputStream, FileInputStream}

    private[this] lazy val blobs = new RandomAccessFile(blobFile.fullpath, "r")
  
    private[this] lazy val indexes = new DataInputStream(new BufferedInputStream(new FileInputStream(indexFile.fullpath)))

    private[this] lazy val idsToOffset = {
      val map = mutable.Map[Long, Long]()
      var done = false
      while (!done) {
        try {
          val id = indexes.readLong()
          val offset = indexes.readLong()
          map(id) = offset
        } catch { case e: java.io.EOFException =>
          done = true
        }
      }
      map
    }
    
    override def get(key: Long): Option[Array[Byte]] = {
      val offset = idsToOffset.get(key)
      if (offset.isEmpty) return None
      
      blobs.seek(offset.get)
      val id = blobs.readLong()
      val length = blobs.readInt()
      val value = new Array[Byte](length)
      var read = 0
      while (read < length) {
        val n = blobs.read(value, 0, length - read)
        if (n == -1) throw new EOFException("Unexpected end of file reached")
        read += n
      }
      Some(value)
    }

    override def close() {
      blobs.close()
      indexes.close()
    }
  }
  
  trait Writer extends Store.Writer with BaseFile {
    import java.io.{RandomAccessFile, DataOutputStream, BufferedOutputStream, FileOutputStream}
    
    val shards: Sharded
    
    private[chompdb] lazy val nextId = new NextId(shards)

    private[this] lazy val blobs = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(blobFile.fullpath)))
  
    private[this] lazy val indexes = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile.fullpath)))
    
    private[this] var offset = 0L
    
    override def put(value: Array[Byte]): Long = synchronized {
      val id = nextId.nextId()
      blobs.writeLong(id)
      blobs.writeInt(value.length)
      blobs.write(value)
      indexes.writeLong(id)
      indexes.writeLong(offset)
      offset += (8 + 4 + value.length)
      id
    }
  
    override def close() {
      blobs.close()
      indexes.close()
    }
    
    def delete() {
      blobFile.delete()
      indexFile.delete()
    }
  }
}
