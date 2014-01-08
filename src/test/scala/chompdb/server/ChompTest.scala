package chompdb.server

import chompdb._
import chompdb.store._
import chompdb.testing._
import f1lesystem.LocalFileSystem
import java.util.concurrent.ScheduledExecutorService

import org.mockito.Mockito.{ mock, when }
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ChompTest extends WordSpec with ShouldMatchers {
	import TestUtils.stringToByteArray
	import TestUtils.byteArrayToString

	val testName = "ChompTest"

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

	val testCatalog = Catalog("TestCatalog", tmpRemoteRoot.fs, tmpRemoteRoot.root)
	val testDatabase = testCatalog.database("TestDatabase")
	
	val testVersion1 = 1L
	val testVersion1Path = testDatabase.createVersion(testVersion1)
	testDatabase.succeedVersion(1L, 1)

	val testVersion2 = 2L
	val testVersion2Path = testDatabase.createVersion(testVersion2)
	testDatabase.succeedVersion(2L, 1)	

	val testChomp = new Chomp {
		override val databases = Seq(testDatabase)
		override val nodes = Map(
			Node("Node1") -> Endpoint("Endpoint1"),
			Node("Node2") -> Endpoint("Endpoint2")
		)
		override val nodeAlive = mock(classOf[NodeAlive])
		when(nodeAlive.isAlive(Node("Node1"))).thenReturn(true)
		when(nodeAlive.isAlive(Node("Node2"))).thenReturn(false)
		override val replicationFactor = 1
		override val replicationBeforeVersionUpgrade = 1
		override val shardIndex = 0
		override val totalShards = 1
		override val executor = mock(classOf[ScheduledExecutorService])
		override val fs = tmpLocalRoot.fs
		override val rootDir = tmpLocalRoot.root

		val mockedNodeProtocol = mock(classOf[NodeProtocol])
		when(mockedNodeProtocol.availableShards(testDatabase.catalog.name, testDatabase.name))
			.thenReturn(Set((1L, 0), (1L, 1), (2L, 0), (2L, 1)))

		def nodeProtocol = Map(Node("Node1") -> mockedNodeProtocol, Node("Node2") -> mockedNodeProtocol)
	}

	"Chomp" should {
		"initialize the map of Database versions being served" in {
			testChomp.servingVersions should be === Map()

			testChomp.initializeServingVersions()

			testChomp.servingVersions should be === Map(testDatabase -> None)
		}

		"create a local Database for a given database" in {
			val db = testChomp.localDB(testDatabase)

			db.name should be === testDatabase.name
			db.catalog.name should be === testDatabase.catalog.name
			db.catalog.fs should be === testChomp.fs
			db.catalog.base should be === testChomp.rootDir
		}

		"retrieve the latest version to download, if any" in {
			testChomp.getNewVersionNumber(testDatabase) should be === Some(2L)
		}

		"download a database version from a secondary FileSystem" in {
			// Write files to "remote" root directory
			trait TestShardedStore extends ShardedWriter {
				override val baseDir = testVersion1Path
			}

			def newShardedWriter(f: ShardedWriter => Unit) = {
				val store = new TestShardedStore {
					val shardsTotal = 2
					val writers = 1
					val writerIndex = 0
				}
				try f(store)
				finally store.close()
			}

			newShardedWriter { writer => 
				writer.ownedShards.zipWithIndex foreach { case (shardId, index) => 
					val id = writer.put(shardId.toString)
				}
				writer.close()
			}

			// "Download" files to "local" root directory
			testChomp.downloadDatabaseVersion(testDatabase, 1L)	

			// Verify that files were "downloaded"
			(testChomp.rootDir /+ "TestCatalog").exists should be === true
			(testChomp.rootDir /+ "TestCatalog" /+ "TestDatabase").exists should be === true
			(testChomp.rootDir /+ "TestCatalog" /+ "TestDatabase" /+ testVersion1.toString).exists should be === true
			(testChomp.rootDir /+ "TestCatalog" /+ "TestDatabase" / (testVersion1.toString + ".version")).exists should be === true
			(0 until testDatabase
				.versionPath(testVersion1)
				.listFiles
				.filter(_.extension == "blob")
				.size) foreach { n => 
				(testChomp.rootDir /+ "TestCatalog" /+ "TestDatabase" /+ testVersion1.toString / s"$n.index").exists should be === true
				(testChomp.rootDir /+ "TestCatalog" /+ "TestDatabase" /+ testVersion1.toString / s"$n.blob").exists should be === true
			}
		}

		"download the latest database version" in {
			// Write files to "remote" root directory
			trait TestShardedStore extends ShardedWriter {
				override val baseDir = testVersion2Path
			}

			def newShardedWriter(f: ShardedWriter => Unit) = {
				val store = new TestShardedStore {
					val shardsTotal = 2
					val writers = 1
					val writerIndex = 0
				}
				try f(store)
				finally store.close()
			}

			newShardedWriter { writer => 
				writer.ownedShards.zipWithIndex foreach { case (shardId, index) => 
					val id = writer.put(shardId.toString)
				}
				writer.close()
			}

			// Download files for latest version of testDatabase
			testChomp.updateDatabase(testDatabase)

			// Verify that latest version (testVersion2) was downloaded
			(testChomp.rootDir /+ "TestCatalog").exists should be === true
			(testChomp.rootDir /+ "TestCatalog" /+ "TestDatabase").exists should be === true
			(testChomp.rootDir /+ "TestCatalog" /+ "TestDatabase" /+ testVersion2.toString).exists should be === true
			(testChomp.rootDir /+ "TestCatalog" /+ "TestDatabase" / (testVersion2.toString + ".version")).exists should be === true
			(0 until testDatabase
				.versionPath(testVersion1)
				.listFiles
				.filter(_.extension == "blob")
				.size) foreach { n => 
				(testChomp.rootDir /+ "TestCatalog" /+ "TestDatabase" /+ testVersion2.toString / s"$n.index").exists should be === true
				(testChomp.rootDir /+ "TestCatalog" /+ "TestDatabase" /+ testVersion2.toString / s"$n.blob").exists should be === true
			}
		}

		"initialize the set of DatabaseVersionShards available locally" in {
			testChomp.initializeAvailableShards()

			testChomp.availableShards.size should be === 4

			testChomp.availableShards should be === Set(
				DatabaseVersionShard(testDatabase.catalog.name, testDatabase.name, 1L, 0),
				DatabaseVersionShard(testDatabase.catalog.name, testDatabase.name, 1L, 1),	
				DatabaseVersionShard(testDatabase.catalog.name, testDatabase.name, 2L, 0),
				DatabaseVersionShard(testDatabase.catalog.name, testDatabase.name, 2L, 1)							
			)

			// testChomp.availableShards should be === Set((1L, 0), (1L, 1), (2L, 0), (2L, 1))
		}

		"update a database version being served" in {
			testChomp.serveVersion(testDatabase, Some(1L))

			testChomp.servingVersions(testDatabase) should be === Some(1L)

			testChomp.initializeServingVersions()

			testChomp.servingVersions(testDatabase) should be === Some(2L)
		}

		"update the internal map of nodes alive" in {
			testChomp.nodesAlive should be === Map.empty

			testChomp.updateNodesAlive()

			testChomp.nodesAlive should be === Map(
				Node("Node1") -> true,
				Node("Node2") -> false
			)
		}

		"update the internal map of nodes' content" in {
			testChomp.nodesContent should be === Map.empty[Node, Set[DatabaseVersionShard]]

			testChomp.updateNodesContent()

			testChomp.nodesContent should be === Map(
				Node("Node1") -> Set(
					DatabaseVersionShard(testDatabase.catalog.name, testDatabase.name, 1L, 0),
					DatabaseVersionShard(testDatabase.catalog.name, testDatabase.name, 1L, 1),
					DatabaseVersionShard(testDatabase.catalog.name, testDatabase.name, 2L, 0),
					DatabaseVersionShard(testDatabase.catalog.name, testDatabase.name, 2L, 1)					
				),
				Node("Node2") -> Set(
					DatabaseVersionShard(testDatabase.catalog.name, testDatabase.name, 1L, 0),
					DatabaseVersionShard(testDatabase.catalog.name, testDatabase.name, 1L, 1),
					DatabaseVersionShard(testDatabase.catalog.name, testDatabase.name, 2L, 0),
					DatabaseVersionShard(testDatabase.catalog.name, testDatabase.name, 2L, 1)
				)
			)
		}

	}
}
