package chompdb.testing

import f1lesystem.FileSystem

import chompdb.store.VersionedStore

object TestUtils {
  implicit def stringToByteBuffer(s: String): java.nio.ByteBuffer = FileSystem.UTF8.encode(s)
  implicit def stringToByteArray(s: String): Array[Byte] = s.getBytes(FileSystem.UTF8)
  def byteArrayToString(b: Array[Byte]): String = new String(b, FileSystem.UTF8)

  def createEmptyShard(versionedStore: VersionedStore, version: Long) {
    val filename = versionedStore.lastShardNum(version) match{
      case None => 0.toString 
      case Some(s) => (s + 1).toString
    }

    val blobFile = versionedStore.root /+ version.toString / (filename + ".blob")
    val indexFile = versionedStore.root /+ version.toString / (filename + ".index")

    blobFile.touch()
    indexFile.touch()
  }
}
