package chompdb.store

import f1lesystem.FileSystem
import java.nio.ByteBuffer

object Store {
  trait Reader extends java.io.Closeable {
    @throws
    def get(key: Long): Array[Byte]
  }

  trait Writer extends java.io.Closeable {
    def put(value: Array[Byte]): Long
  }

  class NotFoundException(key: Long) extends Exception
}

trait Splits {
  val splitIndex: Int
  val splitFactor: Int
}
