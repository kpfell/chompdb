package chompdb.testing

import f1lesystem.FileSystem

object TestUtils {
  implicit def stringToByteBuffer(s: String): java.nio.ByteBuffer = FileSystem.UTF8.encode(s)
  implicit def stringToByteArray(s: String): Array[Byte] = s.getBytes(FileSystem.UTF8)
  def byteArrayToString(b: Array[Byte]): String = new String(b, FileSystem.UTF8)
}
