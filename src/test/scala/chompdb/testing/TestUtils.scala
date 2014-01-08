package chompdb.testing

import f1lesystem.FileSystem

import chompdb.Database

object TestUtils {
  implicit def stringToByteBuffer(s: String): java.nio.ByteBuffer = FileSystem.UTF8.encode(s)
  implicit def stringToByteArray(s: String): Array[Byte] = s.getBytes(FileSystem.UTF8)
  def byteArrayToString(b: Array[Byte]): String = new String(b, FileSystem.UTF8)

  def createEmptyShard(database: Database, version: Long) {
    val filename = database.versionedStore.lastShardNum(version) match{
      case None => 0.toString 
      case Some(s) => (s + 1).toString
    }

    val blobFile = database.versionedStore.root /+ version.toString / (filename + ".blob")
    val indexFile = database.versionedStore.root /+ version.toString / (filename + ".index")

    blobFile.touch()
    indexFile.touch()
  }
}
