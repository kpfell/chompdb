package chompdb.server

import chompdb._
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import f1lesystem.LocalFileSystem

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChompDBTest extends WordSpec with ShouldMatchers {
	val tmpRemoteRoot = new LocalFileSystem.TempRoot {
		override val rootName = "ChompDBTestRemote"
	}

	val tmpLocalRoot = new LocalFileSystem.TempRoot {
		override val rootName = "ChompDBTestLocal"
	}

	val testCatalog = Catalog("TestCatalog", tmpRemoteRoot.fs, tmpRemoteRoot.root)
	val testDatabase = testCatalog.database("TestDatabase")
	val testVersion = 2L
	val testVersionPath = testDatabase.createVersion(testVersion)
	testDatabase.succeedVersion(2L, 1)

	val testChompDB = new ChompDB(Seq(testDatabase), 1, 1, 0, 1, new ScheduledExecutor(),
		tmpLocalRoot.fs, tmpLocalRoot.root
	)

	"ChompDB" should {
		"retrieve new version number, if any, from database to update local filesystem" in {
			val newVersionNumber = testChompDB.getNewVersionNumber(testDatabase)

			newVersionNumber.get should be === testVersion
		}
	}
}