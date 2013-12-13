package chompdb.server

import chompdb._
import chompdb.store._
import chompdb.testing._
import f1lesystem.LocalFileSystem
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChompDBTest extends WordSpec with ShouldMatchers {
	import TestUtils.stringToByteArray
	import TestUtils.byteArrayToString

	// Creates test ChompDB instance
	val testName = "ChompDBTest"

	val tmpRemoteRoot = new LocalFileSystem.TempRoot {
		override val rootName = "remote"
		override lazy val root: fs.Dir = {
			val tmp = fs.parseDirectory(System.getProperty("java.io.tmpdir")) /+ testName /+ rootName
			if (tmp.exists) {
				tmp.deleteRecursively()
			}
			tmp.mkdir()
			tmp
		}
	}

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

	val testCatalog = Catalog("TestCatalog", tmpRemoteRoot.fs, tmpRemoteRoot.root)
	val testDatabase = testCatalog.database("TestDatabase")
	val testVersion = 2L
	val testVersionPath = testDatabase.createVersion(testVersion)
	testDatabase.succeedVersion(2L, 1)

	val testChompDB = new ChompDB(Seq(testDatabase), 1, 1, 0, 1, new ScheduledExecutor(),
		tmpLocalRoot.fs, tmpLocalRoot.root
	)

	// Populates testVersion with shards
	trait TestShardedStore extends ShardedWriter {
		override val baseDir = testVersionPath
	}

	def newShardedWriter(f: ShardedWriter => Unit) = {
		val store = new TestShardedStore {
			val shardsTotal = 5
			val writers = 1
			val writerIndex = 0
		}
		try f(store)
		finally store.close()
	}

	newShardedWriter { writer  => 
		writer.ownedShards.zipWithIndex foreach { case (shardId, index) => 
			val id = writer.put(shardId.toString)
		}
		writer.close()
	}

	"ChompDB" should {
		"retrieve new version number, if any, from database to update local filesystem" in {
			val newVersionNumber = testChompDB.getNewVersionNumber(testDatabase)

			newVersionNumber.get should be === testVersion
		}

		"download the latest database version" in {
			testChompDB.updateDatabase(testDatabase)

			(testChompDB.rootDir /+ "TestCatalog").exists should be === true
			(testChompDB.rootDir /+ "TestCatalog" /+ "TestDatabase").exists should be === true
			(testChompDB.rootDir /+ "TestCatalog" /+ "TestDatabase" /+ testVersion.toString).exists should be === true
			(testChompDB.rootDir /+ "TestCatalog" /+ "TestDatabase" / (testVersion.toString + ".version")).exists should be === true
			(0 until testDatabase
				.versionedStore
				.versionPath(testVersion)
				.listFiles
				.filter(_.extension == "blob")
				.size) foreach { n => 
				(testChompDB.rootDir /+ "TestCatalog" /+ "TestDatabase" /+ testVersion.toString / s"$n.index").exists should be === true
				(testChompDB.rootDir /+ "TestCatalog" /+ "TestDatabase" /+ testVersion.toString / s"$n.blob").exists should be === true
			}
		}

		"determine whether a version exists locally" in {
			testChompDB.versionExists(testDatabase, 1L) should be === false
			testChompDB.versionExists(testDatabase, 2L) should be === true
		}
	}
}