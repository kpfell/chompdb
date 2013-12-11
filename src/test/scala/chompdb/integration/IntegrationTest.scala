package chompdb.integration

import chompdb._
import chompdb.sharding._
import chompdb.testing._
import f1lesystem.LocalFileSystem
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import scala.collection._
import chompdb.store.ShardedWriter
import f1lesystem.FileSystem
import chompdb.store.Store
import scala.collection.mutable.SynchronizedSet
import scala.collection.mutable.SynchronizedBuffer

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class IntegrationTest extends WordSpec with ShouldMatchers {
  import TestUtils.stringToByteArray
  import TestUtils.byteArrayToString

  val numThreads = 5

  val tmpRoot = new LocalFileSystem.TempRoot {
    override val rootName = "IntegrationTest"
  }
  
  "ChompDB" should {
    "create stores and upload them to S3" in {
      
      val localDir = tmpRoot.root /+ "local"
      localDir.mkdir()
      
      val writers = (0 until numThreads) map { i =>
        new ShardedWriter {
          val writers = numThreads
          val writerIndex = i
          val shardsTotal = 20
          val baseDir = localDir
        }
      }
      
      val ids = new mutable.HashSet[Long] with SynchronizedSet[Long]
      
      val threads = (1 to numThreads) map { n =>
        new Thread("IntegrationTest") {
          override def run() {
            (1 to 100) foreach { x =>
              val id = writers(n-1).put(s"This is a test: thread $n element $x")
              ids += id
            }            
          }
        }
      }
      
      threads foreach { _.start() }
      threads foreach { _.join() }
      
      writers foreach { _.close() }

      ids should be === Set((0 until 500): _*)
      
      val remoteDir = tmpRoot.root /+ "remote"
      val c = Catalog("catalog1", tmpRoot.fs, remoteDir)
      val d = c.database("database1")
      val version = System.currentTimeMillis // timestamp
      val versionPath = d.createVersion(version)
      copyShards(writers, versionPath)
      d.succeedVersion(version, writers(0).shardsTotal)
      
      def copyShards(writers: Seq[ShardedWriter], versionDir: FileSystem#Dir) {
        for (w <- writers) {
          for (baseFile <- w.shardFiles) {
            copy(baseFile.indexFile, versionDir / baseFile.indexFile.filename)
            copy(baseFile.blobFile,  versionDir / baseFile.blobFile.filename)
          }
        }
      }
      
      def copy(from: FileSystem#File, to: FileSystem#File) {
        from.readAsReader { reader =>
          to.write(reader, from.size)
        }
      }
      
      (remoteDir /+ "catalog1").exists should be === true
      (remoteDir /+ "catalog1" /+ "database1").exists should be === true
      (remoteDir /+ "catalog1" /+ "database1" /+ (version.toString)).exists should be === true
      (0 until 20) foreach { n =>
        (remoteDir /+ "catalog1" /+ "database1" /+ (version.toString) / s"$n.index").exists should be === true
        (remoteDir /+ "catalog1" /+ "database1" /+ (version.toString) / s"$n.blob").exists should be === true
      }
    }
  }
}