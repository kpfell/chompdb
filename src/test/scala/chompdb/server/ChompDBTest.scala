package chompdb.server

import chompdb._
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import f1lesystem.LocalFileSystem

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChompDBTest extends WordSpec with ShouldMatchers {
	// TODO: Merge temporary file directories created for this test under one directory
	// i.e. dir > (local, remote)
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

		// TODO: Update testVersion so that it has multiple shards for this test to verify
		"download a database version" in {
			testChompDB.downloadDatabaseVersion(testDatabase, testVersion)

			(testChompDB.rootDir /+ "TestCatalog").exists should be === true
			(testChompDB.rootDir /+ "TestCatalog" /+ "TestDatabase").exists should be === true
			(testChompDB.rootDir /+ "TestCatalog" /+ "TestDatabase" /+ testVersion.toString).exists should be === true
			(testChompDB.rootDir /+ "TestCatalog" /+ "TestDatabase" / (testVersion.toString + ".version")).exists should be === true
			(0 until testDatabase
				.versionedStore
				.versionPath(testVersion)
				.listFiles
				.size) foreach { n =>
				(testChompDB.rootDir /+ "TestCatalog" /+ "TestDatabase" /+ testVersion.toString / s"$n.index").exists should be === true
				(testChompDB.rootDir /+ "TestCatalog" /+ "TestDatabase" /+ testVersion.toString / s"n.blob").exists should be === true
			}
		}

		"determine whether a version exists locally" in {
			testChompDB.versionExists(testDatabase, 1L) should be === false
			testChompDB.versionExists(testDatabase, 2L) should be === true
		}
	}
}