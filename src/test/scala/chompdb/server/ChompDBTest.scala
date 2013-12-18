package chompdb.server

import chompdb._
import chompdb.store._
import chompdb.testing._
import f1lesystem.LocalFileSystem
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import pixii.fake._
import scala.concurrent.duration._

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

	val testNodeAlive = new DynamoNodeAlive {
		override def isAlive(node: Node): Boolean = true
		override def imAlive(): Unit = println("alive")

		val gracePeriod = Duration(10, SECONDS)
		val tableName = "testTable"
		protected val dynamoDB = new FakeDynamo()
	}

	val testChompDB = new ChompDB {

		val databases = Seq(testDatabase)
		val nodes = Map(Node("node1") -> Endpoint("endpointvalue"))
		val nodeProtocol = new NodeProtocol()
		val nodeAlive = testNodeAlive
		val replicationFactor = 1
		val replicationFactorBeforeVersionUpgrade = 1 
		val shardIndex = 0
		val totalShards = 1
		val executor = Executors.newScheduledThreadPool(1)
		val fs = tmpLocalRoot.fs
		val rootDir = tmpLocalRoot.root
	}

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
		"retrieve the latest version to download, if any" in {
			testChompDB.getNewVersionNumber(testDatabase) should be === Some(testVersion)
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
	}
}