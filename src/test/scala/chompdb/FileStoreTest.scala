package chompdb

import f1lesystem.LocalFileSystem

import java.io.File

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

import scala.collection._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class FileStoreTest extends WordSpec with ShouldMatchers {
  import TestUtils.stringToByteArray
  import TestUtils.byteArrayToString

  "FileStore" should {
    "write and read back data" in {
      val writer = new FileStore.Writer with LocalFileSystem.TempRoot {
        override val rootName = "FileSystemWriterTest"
        override val baseFile = root / "test"
        override val shards = new Sharded {
          override val shardsIndex = 2
          override val shardsTotal = 3
        }
      }
      
      val id1 = writer.put("foo")
      id1 should be === 2
      val id2 = writer.put("bar")
      id2 should be === 5
      writer.close()

      val reader = new FileStore.Reader {
        override val baseFile = writer.baseFile
      }
      reader.get(id1) map byteArrayToString should be === Some("foo")
      reader.get(id2) map byteArrayToString should be === Some("bar")
    }

    "support large amounts of data" in {
      val writer = new FileStore.Writer with LocalFileSystem.TempRoot {
        override val rootName = "FileSystemWriterTest"
        override val baseFile = root / "test"
        override val shards = new Sharded {
          override val shardsIndex = 2
          override val shardsTotal = 3
        }
      }

      val blob = "abcdefghijklmnopqrstuvwxyz"
      val map = mutable.Map[Int, Long]()
      
      locally {
        for (i <- 1 to 100000) {
          map(i) = writer.put(blob + "-" + i)
        }
        writer.close()
      }
      
      locally {
        val reader = new FileStore.Reader {
          override val baseFile = writer.baseFile
        }
        for (i <- 1 to 100000) {
          reader.get(map(i)) map byteArrayToString should be === Some(blob + "-" + i)
        }
        reader.close()
      }
    }

    "not leak file descriptors when reading/writing" in {
      val tmp = new LocalFileSystem.TempRoot {
        override val rootName = "FileSystemWriterTest-Leak"
      }
      
      for (i <- 1 to 99999) {
        val writer = new FileStore.Writer {
          override val baseFile = tmp.root / s"test-$i"
          override val shards = new Sharded {
            override val shardsIndex = 2
            override val shardsTotal = 3
          }
        }
        val id = writer.put(s"hello-$i")
        writer.close()
        
        val reader = new FileStore.Reader {
          override val baseFile = writer.baseFile
        }
        reader.get(id) map byteArrayToString should be === Some(s"hello-$i")
        reader.close()
        
        writer.delete()
      }
    }
  }
}