package chompdb.store

import chompdb.sharding._
import f1lesystem.FileSystem
import java.io._
import scala.collection._

object FileStore {

  private[FileStore] trait BaseFile {
    val baseFile: FileSystem#File

    lazy val blobFile  = baseFile.parent / s"${baseFile.filename}.blob"
    lazy val indexFile = baseFile.parent / s"${baseFile.filename}.index"
  }

  trait Reader extends Store.Reader with BaseFile {
    import java.io.{RandomAccessFile, DataInputStream, BufferedInputStream, FileInputStream}

    private[this] lazy val blobs = new RandomAccessFile(blobFile.fullpath, "r")

    private[this] lazy val (ids, offsets) = {
      val indexes = new DataInputStream(new BufferedInputStream(new FileInputStream(indexFile.fullpath)))
      try {
        val count = (indexFile.size / 16).toInt
        val ids = new Array[Long](count)
        val offsets = new Array[Long](count)
        var i = 0
        while (i < count) {
          ids(i) = indexes.readLong()
          offsets(i) = indexes.readLong()
          i += 1
        }
        (ids, offsets)
      } finally indexes.close()
    }

    override def get(key: Long): Array[Byte] = {
      val i = java.util.Arrays.binarySearch(ids, key)
      if (ids(i) != key) throw new Store.NotFoundException(key)
      blobs.seek(offsets(i))
      val id = blobs.readLong()
      val length = blobs.readInt()
      val value = new Array[Byte](length)
      var read = 0
      while (read < length) {
        val n = blobs.read(value, 0, length - read)
        if (n == -1) throw new EOFException("Unexpected end of file reached")
        read += n
      }
      value
    }

    override def close() {
      blobs.close()
    }
  }

  trait Writer extends Store.Writer with BaseFile {
    import java.io.{RandomAccessFile, DataOutputStream, BufferedOutputStream, FileOutputStream}

    val shards: Sharded

    private[chompdb] lazy val nextId = new IdGenerator(shards)

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
