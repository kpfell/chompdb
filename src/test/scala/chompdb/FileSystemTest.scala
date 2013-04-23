package chompdb

import org.scalatest.{OneInstancePerTest, WordSpec}
import org.scalatest.matchers.ShouldMatchers
import java.io.File

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class FileSystemTest extends WordSpec with OneInstancePerTest with ShouldMatchers {
  val tempRoot = new TempRoot {
    override val rootName = classOf[FileSystemTest].getClass.getSimpleName
  }
  val fs = tempRoot.fs
  val tmp = tempRoot.root

  "LocalFileSystem" should {
    "parse directory" in {
      fs.parseDirectory(tmp.fullpath) should be === tmp
    }

    "parse file" in {
      val foo = tmp / "foo"
      fs.parseFile(foo.fullpath) should be === foo
    }

    "return file basename and extesion" in {
      val foo = tmp / "foo"
      foo.basename should be === "foo"
      foo.parent should be === tmp
      foo.extension should be === ""

      val bar = tmp / "bar.ext"
      bar.basename should be === "bar"
      bar.parent should be === tmp
      bar.extension should be === "ext"

      val tgz = tmp / "archive.tar.gz"
      tgz.basename should be === "archive"
      tgz.extension should be === "tar.gz"
    }

    "return empty directories and files for empty directory" in {
      tmp.listDirectories should be === Seq.empty
      tmp.listFiles should be === Seq.empty
    }

    "create directory using mkdir()" in {
      tmp.exists should be === true

      val subDir = tmp /+ "subDir"
      subDir.exists should be === false

      subDir.mkdir()
      subDir.exists should be === true
    }

    "create file using touch()" in {
      val subFile = tmp / "subFile"
      subFile.exists should be === false

      subFile.touch()
      subFile.exists should be === true
    }

    "delete directory" in {
      tmp.exists should be === true

      val subDir = tmp /+ "subDir"
      subDir.exists should be === false

      subDir.mkdir()
      subDir.exists should be === true

      subDir.delete()
      subDir.exists should be === false
    }

    "delete file" in {
      val subFile = tmp / "subFile"
      subFile.exists should be === false

      subFile.touch()
      subFile.exists should be === true

      subFile.delete()
      subFile.exists should be === false
    }

    "list files and directories" in {
      val subDir1 = tmp /+ "subDir1"
      subDir1.mkdir()

      val subDir2 = subDir1 /+ "subDir2"
      subDir2.mkdir()

      val subDir3 = subDir1 /+ "subDir3"
      subDir3.mkdir()

      val subDir4 = subDir3 /+ "subDir4"
      subDir4.mkdir()

      val subFile1 = subDir2 / "subFile1"
      subFile1.touch()

      val subFile2 = subDir2 / "subFile2"
      subFile2.touch()

      val subFile3 = subDir3 / "subFile3"
      subFile3.touch()

      subDir1.listFiles.toSet should be === Set()
      subDir1.listDirectories.toSet should be === Set(subDir2, subDir3)

      subDir2.listFiles.toSet should be === Set(subFile1, subFile2)
      subDir2.listDirectories.toSet should be === Set()

      subDir3.listFiles.toSet should be === Set(subFile3)
      subDir3.listDirectories.toSet should be === Set(subDir4)
    }

    "delete files and directories recursively" in {
      val subDir1 = tmp /+ "subDir1"
      subDir1.mkdir()

      val subDir2 = subDir1 /+ "subDir2"
      subDir2.mkdir()

      val subDir3 = subDir1 /+ "subDir3"
      subDir3.mkdir()

      val subFile1 = subDir2 / "subFile1"
      subFile1.touch()

      val subFile2 = subDir2 / "subFile2"
      subFile2.touch()

      subDir1.deleteRecursively()

      subDir1.exists should be === false
      subDir2.exists should be === false
      subDir3.exists should be === false

      subFile1.exists should be === false
      subFile2.exists should be === false
    }

    "generate paths" in {
      val foo = tmp /+ "foo" / "bar"
      (foo.fullpath drop tmp.fullpath.length) should be === "/foo/bar"
    }

    "read and write to a file using strings" in {
      val foo = tmp / "foo"
      foo.write("foo")
      foo.readAsString() should be === "foo"
    }
  }
}