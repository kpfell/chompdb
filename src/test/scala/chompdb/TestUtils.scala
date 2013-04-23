package chompdb

object TestUtils {
  implicit def stringToByteBuffer(s: String): java.nio.ByteBuffer = FileSystem.UTF8.encode(s)
}

trait TempRoot {
  val rootName: String
  
  val fs = new LocalFileSystem()

  // create and return empty test directory under java.io.tmpdir
  lazy val root: fs.Dir = {
    val tmp = fs.parseDirectory(System.getProperty("java.io.tmpdir")) /+ rootName
    if (tmp.exists) {
      tmp.deleteRecursively()
    }
    tmp.mkdir()
    tmp
  }
  
}