package chompdb.store

import chompdb._
import chompdb.sharding._
import chompdb.testing._
import f1lesystem.LocalFileSystem
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import scala.collection._
import org.scalatest.OneInstancePerTest

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class VersionedStoreTest extends WordSpec with ShouldMatchers with OneInstancePerTest {
  import TestUtils.stringToByteArray
  import TestUtils.byteArrayToString

  "VersionedStore" should {

    val vs = new VersionedStore with LocalFileSystem.TempRoot {
      override val rootName = classOf[VersionedStoreTest].getSimpleName
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
    "create version file containing file manifest and total shards" in {
      vs.succeedVersion(1L, 0)
      vs.versionMarker(1L).readAsString() should be === "Total Shards: 0\n"
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

    "cleanup older versions" in {
      vs.succeedVersion(1L, 1)
      vs.succeedVersion(2L, 1)
      vs.succeedVersion(3L, 1)

      vs.cleanup(versionsToKeep = 2)

      vs.versions should be === Seq(3L, 2L)
      vs.mostRecentVersion should be === Some(3L)
    }
  }
}