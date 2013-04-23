package chompdb

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

import java.io.File

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class FileSystemWriterTest extends WordSpec with ShouldMatchers {
  import TestUtils.stringToByteBuffer

  "FileSystemWrite" should {
    "calculate file location from long id" in {
      val writer = new FileSystemWriter with TempRoot{
        override val rootName = "FileSystemWriterTest"
        override val shardsIndex = 1
        override val shardsTotal = 1
      }

      writer.fileForId(0).fullpath should endWith ("/FileSystemWriterTest/00")
      writer.fileForId(1).fullpath should endWith ("/FileSystemWriterTest/01")

      writer.fileForId(255).fullpath should endWith ("/FileSystemWriterTest/ff")
      writer.fileForId(256).fullpath should endWith ("/FileSystemWriterTest/D01/00")
      writer.fileForId(257).fullpath should endWith ("/FileSystemWriterTest/D01/01")

      writer.fileForId(-1).fullpath should endWith ("/FileSystemWriterTest/Dff/Dff/Dff/Dff/Dff/Dff/Dff/ff")

      writer.fileForId(Long.MinValue).fullpath should endWith ("/FileSystemWriterTest/D80/D00/D00/D00/D00/D00/D00/00")
      writer.fileForId(Long.MaxValue).fullpath should endWith ("/FileSystemWriterTest/D7f/Dff/Dff/Dff/Dff/Dff/Dff/ff")
    }

    "write values" in {
      val writer = new FileSystemWriter with TempRoot {
        override val rootName = "FileSystemWriterTest"
        override val shardsIndex = 1
        override val shardsTotal = 1
      }

      for (i <- 1 to 99) {
        val id = writer.put("hello-" + i)
        id should be === i

        writer.fileForId(id).readAsString() should be === ("hello-" + i)
      }
    }

    // currently ignored because too long (approx 20 secs)
    "not leak file descriptors" ignore {
      val writer = new FileSystemWriter with TempRoot {
        override val rootName = "FileSystemWriterTest"
        override val shardsIndex = 1
        override val shardsTotal = 1
      }

      for (i <- 1 to 99999) {
        val id = writer.put("hello-" + i)
        id should be === i

        writer.fileForId(id).readAsString() should be === ("hello-" + i)
      }
    }

    "write for different shards (and increment ids accordingly)" in {
      val writer = new FileSystemWriter with TempRoot {
        override val rootName = "FileSystemWriterTest"
        override val shardsIndex = 2
        override val shardsTotal = 3
      }

      for (i <- 2 to 99 by 3) {
        val id = writer.put("hello-" + i)
        id should be === i

        writer.fileForId(id).readAsString() should be === ("hello-" + i)
      }
    }
  }
}