package chompdb.integration

import chompdb._
import chompdb.store._
import f1lesystem._
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicBoolean

trait DatabaseCreator { 
  /** Directory where databases are (locally) created */
  val localDir: FileSystem#Dir

  val delayBetweenVersions: Duration
  
  val numThreads: Int

  val params: Params
  
  private val random = new RandomUtils(); import random._

  val creatorDir = localDir /+ "creator"

  private val stopFlag = new AtomicBoolean(false)
  
  def run() {
    while (!stopFlag.get()) {
      val (db, dbParams) = pick(params.databases)

      val newVersion = System.currentTimeMillis
    
      println(s"Creating new database $db version $newVersion ...")
      
      val writers = (0 until numThreads) map { i =>
        new ShardedWriter {
          val writers = numThreads
          val writerIndex = i
          val shardsTotal = dbParams.shardsTotal
          val baseDir: FileSystem#Dir = creatorDir
        }
      }
 
      val threads = writers.zipWithIndex map { case (writer, writerIndex) =>
        new Thread(s"DatabaseCreator-$writer") {
          override def run() {
            var blobIndex = writerIndex
            while (writerIndex < dbParams.nElements) {
              val size = randomInt(dbParams.blobSizeRange._1, dbParams.blobSizeRange._2)
              val blob = Blob(Blob.Data(db.name, newVersion, writerIndex, writerIndex , size))
              val id = writer.put(blob)
              // println(s"Wrote blob #$id")
              blobIndex += writers.size
            }            
          }
        }
      }
      
      threads foreach { _.start() }
      threads foreach { _.join() }
      
      writers foreach { _.close() }

      
      def copyShards(writers: Seq[ShardedWriter], versionDir: FileSystem#Dir) {
        for (w <- writers) {
          for (baseFile <- w.shardFiles) {
            copy(baseFile.indexFile, versionDir / baseFile.indexFile.filename)
            copy(baseFile.blobFile,  versionDir / baseFile.blobFile.filename)
            db.versionedStore.succeedShard(newVersion, baseFile.baseFile.basename.toInt)
          }
        }
      }
      
      def copy(from: FileSystem#File, to: FileSystem#File) {
        from.readAsReader { to.write(_, from.size) }
      }
      
      val versionPath = db.versionedStore.createVersion(newVersion)
      
      println(s"Copying $numThreads shards ...")
      copyShards(writers, versionPath)
      db.versionedStore.succeedVersion(newVersion, dbParams.shardsTotal)
      
      Thread.sleep(delayBetweenVersions.toMillis)
    }
  }

  def stop() {
    stopFlag.set(true)
  }
}
