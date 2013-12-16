package chompdb.store

import f1lesystem.FileSystem
import java.io._
import java.util.Properties

object VersionedStore {
  val versionSuffix = ".version"
}

trait VersionedStore {
  import VersionedStore._

  val fs: FileSystem

  val root: fs.Dir

  def versionPath(version: Long) = root /+ version.toString

  def mostRecentVersionPath = mostRecentVersion map versionPath

  def mostRecentVersion: Option[Long] = versions.headOption

  def countShardsInVersion(version: Long): Int = versionPath(version)
    .listFiles
    .map(_.filename)
    .filter(_.endsWith(".blob"))
    .size

  def createVersion(version: Long = System.currentTimeMillis): fs.Dir = {
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

  def succeedVersion(version: Long, shardsTotal: Int) {
    versionMarker(version).touch()
    val files = versionPath(version).listFiles
    val marker = versionMarker(version)

    val props = new Properties()
    props.put("shardsTotal", shardsTotal.toString)
    props.put("fileManifest", files.toString)

    // TODO: .write and .store are both void functions, figure out how to nest them
    val fileOutputStream = new FileOutputStream(marker.fullpath)
    props.store(fileOutputStream, "")
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

  /** Sorted from most recent to oldest */
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

  def versionMarker(version: Long) = root / (version.toString + versionSuffix)

  def parseVersion(p: FileSystem#Path): Option[Long] = {
    if (p.filename endsWith versionSuffix) {
      Some((p.filename dropRight versionSuffix.length).toLong)
    } else {
      try Some(p.filename.toLong) catch { case e: Exception => None }
    }
  }
}
