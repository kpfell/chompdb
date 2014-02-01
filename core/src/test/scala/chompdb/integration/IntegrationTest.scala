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

  val testName = getClass.getSimpleName
  val tmpRoot = LocalFileSystem.tempRoot(testName)
  val tmpLocalRoot = tmpRoot /+ "local"
  val tmpRemoteRoot = tmpRoot /+ "remote"
  
  Seq(tmpLocalRoot, tmpRemoteRoot) foreach { _.mkdir() }
  
  "ChompDB" should {
    "create stores and upload them to S3" in {
      
      val writers = (0 until numThreads) map { i =>
        new ShardedWriter {
          val writers = numThreads
          val writerIndex = i
          val shardsTotal = 20
          val baseDir = tmpLocalRoot
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

      ids should be === (0 until 500).toSet
      
      val c = Catalog("catalog1", tmpRemoteRoot)
      val d = c.database("database1")
      val version = System.currentTimeMillis // timestamp
      val versionPath = d.versionedStore.createVersion(version)
      copyShards(writers, versionPath)
      d.versionedStore.succeedVersion(version, writers(0).shardsTotal)
      
      def copyShards(writers: Seq[ShardedWriter], versionDir: FileSystem#Dir) {
        for (w <- writers) {
          for (baseFile <- w.shardFiles) {
            copy(baseFile.indexFile, versionDir / baseFile.indexFile.filename)

            copy(baseFile.blobFile,  versionDir / baseFile.blobFile.filename)

            if ((versionDir / baseFile.indexFile.filename).exists && 
                (versionDir / baseFile.blobFile.filename).exists)
              d.versionedStore.succeedShard(version, baseFile.baseFile.basename.toInt)
          }
        }
      }
      
      def copy(from: FileSystem#File, to: FileSystem#File) {
        from.readAsReader { reader =>
          to.write(reader, from.size)
        }
      }
      
      (tmpRemoteRoot /+ "catalog1").exists should be === true
      (tmpRemoteRoot /+ "catalog1" /+ "database1").exists should be === true
      (tmpRemoteRoot /+ "catalog1" /+ "database1" /+ (version.toString)).exists should be === true
      (0 until 20) foreach { n =>
        (tmpRemoteRoot /+ "catalog1" /+ "database1" /+ (version.toString) / s"$n.index").exists should be === true
        (tmpRemoteRoot /+ "catalog1" /+ "database1" /+ (version.toString) / s"$n.blob").exists should be === true
        (tmpRemoteRoot /+ "catalog1" /+ "database1" /+ (version.toString) / s"$n.shard").exists should be === true
      }
    }
  }
}