package chompdb.store

import chompdb._
import chompdb.sharding._
import chompdb.testing._
import f1lesystem.LocalFileSystem
import java.io._
import java.util.Properties
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import scala.collection._
import org.scalatest.OneInstancePerTest

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class VersionedStoreTest extends WordSpec with ShouldMatchers with OneInstancePerTest {
  import TestUtils.stringToByteArray
  import TestUtils.byteArrayToString
  import TestUtils.createEmptyShard

  "VersionedStore" should {

    val vs = new VersionedStore {
      override val fs = LocalFileSystem
      override val root = LocalFileSystem.tempRoot(classOf[VersionedStoreTest].getSimpleName)
    }

    "create versions" in {
      vs.mostRecentVersion should be === None

      val dir = vs.createVersion(1L)
      vs.succeedVersion(1L, 1)
      vs.mostRecentVersion should be === Some(1L)
    }

    "delete versions" in {
      val dir = vs.createVersion(1L)
      (dir / "foo").touch()
      vs.succeedVersion(1L, 1)

      vs.deleteVersion(1L)
      vs.versions should be === Seq.empty
    }

    // TO DO: Revise vs so that this test can actually check for shard files
    // TO DO: Figure out how to revise VersionedStore to omit FileInputStream
    "create version file containing file manifest and total shards" in {
      vs.succeedVersion(1L, 1)

      val props = new Properties()
      props.put("shardsTotal", "1")
      props.put("fileManifest", Seq().toString)

      val succeededProps = new Properties()
      val fileInputStream = new FileInputStream(vs.versionMarker(1L).fullpath)

      succeededProps.load(fileInputStream)

      succeededProps.getProperty("shardsTotal") should be === "1"
      succeededProps.getProperty("fileManifest") should be === "List()"
    }

    "return versions in reverse chronological order" in {
      vs.succeedVersion(2L, 1)
      vs.versions should be === Seq(2L)
      vs.mostRecentVersion should be === Some(2L)

      vs.succeedVersion(1L, 1)
      vs.versions should be === Seq(2L, 1L)
      vs.mostRecentVersion should be === Some(2L)

      vs.succeedVersion(3L, 1)
      vs.versions should be === Seq(3L, 2L, 1L)
      vs.mostRecentVersion should be === Some(3L)
    }

    "return the shard numbers for a given version" in {
      vs.createVersion(4L)
      createEmptyShard(vs, 4L)
      createEmptyShard(vs, 4L)
      createEmptyShard(vs, 4L)

      vs.shardNumsForVersion(4L) should be === Set(0, 1, 2)
    }

    "cleanup older versions" in {
      vs.succeedVersion(1L, 1)
      vs.succeedVersion(2L, 1)
      vs.succeedVersion(3L, 1)
      vs.succeedVersion(4L, 3)

      vs.cleanup(versionsToKeep = 2)

      vs.versions should be === Seq(4L, 3L)
      vs.mostRecentVersion should be === Some(4L)
    }
  }
}