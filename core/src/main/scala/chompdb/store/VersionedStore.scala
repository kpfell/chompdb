package chompdb.store

import chompdb.Database
import chompdb.DatabaseVersionShard

import f1lesystem.FileSystem
import java.io._
import java.util.Properties

object VersionedStore {
  val shardIndexSuffix = ".shardIndex"
  val shardSuffix = ".shard"
  val versionSuffix = ".version"
}

trait VersionedStore {
  import VersionedStore._

  val root: FileSystem#Dir

  def versionPath(version: Long) = root /+ version.toString

  def mostRecentVersionPath = mostRecentVersion map versionPath

  def mostRecentVersion: Option[Long] = versions.headOption

  def shardNumsForVersion(version: Long): Set[Int] = versionPath(version)
    .listFiles
    .filter(_.extension == "shard")
    .map { _.basename.toInt }
    .toSet 

  def createVersion(version: Long = System.currentTimeMillis): FileSystem#Dir = {
    if (versions contains version) throw new RuntimeException("Version already exists")
    val path = versionPath(version)
    path.deleteRecursively() // in case there's an incomplete version already
    path.mkdir()
    path
  }

  def deleteVersion(version: Long) {
    versionPath(version).deleteRecursively()
    versionMarker(version).delete()
  }

  def numShardsForVersion(version: Long): Int = {
    val marker = versionMarker(version)

    val vmInput = new FileInputStream(marker.fullpath)
    val props = new Properties()
    props.load(vmInput)
    vmInput.close()

    props.getProperty("shardsTotal", "0").toInt
  }

  def succeedShard(version: Long, shard: Int) {
    shardMarker(version, shard).touch()
  }

  def succeedShardIndex(version: Long, shardIndex: Int) {
    shardIndexMarker(version).touch()
    val marker = shardIndexMarker(version)

    val props = new Properties()
    props.put("shardIndex", shardIndex.toString)

    val fileOutputStream = new FileOutputStream(marker.fullpath)
    props.store(fileOutputStream, null)
    fileOutputStream.close()
  }

  def succeedVersion(version: Long, shardsTotal: Int) {
    versionMarker(version).touch()
    val files = versionPath(version).listFiles
    val marker = versionMarker(version)

    val props = new Properties()
    props.put("shardsTotal", shardsTotal.toString)
    props.put("fileManifest", files.toString)

    val fileOutputStream = new FileOutputStream(marker.fullpath)
    props.store(fileOutputStream, null)
    fileOutputStream.close()
  }

  def cleanup(versionsToKeep: Int) {
    val keepers = versions.take(versionsToKeep).toSet
    for (p <- root.listDirectories) {
      val v = parseVersion(p)
      if (v.isDefined && !(keepers contains v.get)) {
        p.deleteRecursively()
      }
    }

    val rejects = versions.drop(versionsToKeep).toSet
    for (r <- rejects) {
      deleteVersion(r)
    }
  }

  def isInconsistentShard(v: Long, f: FileSystem#File): Boolean = {
    (f.extension == "blob" || f.extension == "index") &&
      !(versionPath(v) / (f.basename + ".shard")).exists
  } 

  def deleteIncompleteShards(version: Long) {
    versionPath(version)
      .listFiles
      .filter { _.basename forall Character.isDigit }
      .foreach { f => 
        if (isInconsistentShard(version, f)) {
          f.delete()
        }
      }
  }

  /* Sorted from most recent to oldest */
  def versions = {
    if (!root.exists) {
      Seq.empty
    } else {
      val vs = for (p <- root.listFiles) yield {
        if (p.filename endsWith versionSuffix) {
          Seq(parseVersion(p).get)
        } else Seq.empty
      }
      vs.flatten.sorted.reverse
    }
  }

  def versionExists(version: Long): Boolean = versions.contains(version)

  def shardIndexMarker(version: Long) = root / (version.toString + shardIndexSuffix)

  def shardMarker(version: Long, shard: Int) = root /+ version.toString / (shard.toString + shardSuffix)

  def versionMarker(version: Long) = root / (version.toString + versionSuffix)

  def parseVersion(p: FileSystem#Path): Option[Long] = {
    if (p.filename endsWith versionSuffix) {
      Some((p.filename dropRight versionSuffix.length).toLong)
    } else {
      try Some(p.filename.toLong) catch { case e: Exception => None }
    }
  }
}