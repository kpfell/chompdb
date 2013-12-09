package chompdb.store

import f1lesystem.FileSystem

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

  def succeedVersion(version: Long) {
    versionMarker(version).touch()
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
