package chompdb

import chompdb._
import f1lesystem.LocalFileSystem

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class DatabaseTest extends WordSpec with ShouldMatchers {
  val testName = "DatabaseTest"

  val tmpLocalRoot = new LocalFileSystem.TempRoot {
    override val rootName = "local"
    override lazy val root: fs.Dir = {
      val tmp = fs.parseDirectory(System.getProperty("java.io.tmpdir")) /+ testName /+ rootName
      if (tmp.exists) {
        tmp.deleteRecursively()
      }
      tmp.mkdir()
      tmp
    }
  }

  val cat = Catalog("TestCatalog", tmpLocalRoot.fs, tmpLocalRoot.root)
  val db1 = cat.database("TestDatabase1")
  val db2 = cat.database("TestDatabase2")

  db1.createVersion(1L)
  db2.createVersion(1L)

  "Database" should {
    "create an empty shard blob and index file" in {
      db1.versionPath(1L).listFiles should be === Seq.empty

      db1.createEmptyShard(1L)

      db1.versionPath(1L).listFiles.size should be === 2
      (db1.versionPath(1L) / "0.blob").exists should be === true
      (db1.versionPath(1L) / "0.index").exists should be === true
    }

    "return the number of the last DatabaseVersionShard, if any" in {
      db2.lastShardNum(1L).getOrElse(false) should be === false

      db2.createEmptyShard(1L)

      db2.lastShardNum(1L).getOrElse(false) should be === 0
    }

    "return the set of DatabaseVersionShards for a given Database version number" in {
      db1.retrieveShards(1L).size should be === 1
    }
  }
}