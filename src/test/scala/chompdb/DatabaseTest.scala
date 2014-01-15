package chompdb

import chompdb._
import chompdb.testing._
import f1lesystem.LocalFileSystem
import chompdb.testing.TestUtils.createEmptyShard

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class DatabaseTest extends WordSpec with ShouldMatchers {
  import TestUtils.lastShardNum

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

  db1.versionedStore.createVersion(1L)

  "Database" should {
    "return the number of the last DatabaseVersionShard, if any" in {
      lastShardNum(db1.versionedStore, 1L).getOrElse(false) should be === false

      createEmptyShard(db1.versionedStore, 1L)

      lastShardNum(db1.versionedStore, 1L).getOrElse(false) should be === 0
    }

    "return the set of DatabaseVersionShards for a given Database version number" in {
      db1.shardsOfVersion(1L) should be === Set(DatabaseVersionShard(db1.catalog.name, db1.name, 1L, 0))
    }
  }
}