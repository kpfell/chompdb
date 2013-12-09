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
      vs.succeedVersion(1L)
      vs.mostRecentVersion should be === Some(1L)
    }

    "delete versions" in {
      val dir = vs.createVersion(1L)
      (dir / "foo").touch()
      vs.succeedVersion(1L)

      vs.deleteVersion(1L)
      vs.versions should be === Seq.empty
    }

    "return versions in reverse chronological order" in {
      vs.succeedVersion(2L)
      vs.versions should be === Seq(2L)
      vs.mostRecentVersion should be === Some(2L)

      vs.succeedVersion(1L)
      vs.versions should be === Seq(2L, 1L)
      vs.mostRecentVersion should be === Some(2L)

      vs.succeedVersion(3L)
      vs.versions should be === Seq(3L, 2L, 1L)
      vs.mostRecentVersion should be === Some(3L)
    }

    "cleanup older versions" in {
      vs.succeedVersion(1L)
      vs.succeedVersion(2L)
      vs.succeedVersion(3L)

      vs.cleanup(versionsToKeep = 2)

      vs.versions should be === Seq(3L, 2L)
      vs.mostRecentVersion should be === Some(3L)
    }
  }
}