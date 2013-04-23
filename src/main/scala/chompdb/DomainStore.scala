package chompdb

trait ShardSet

class ShardSetImpl(root: FileSystem#Path, domain: DomainInfo)

trait WithDomainInfo extends DomainInfo {
  val info: DomainInfo
  override val numShards = info.numShards
  override val store = info.store
  override val shardingScheme = info.shardingScheme
}


trait DomainStore extends WithDomainInfo {
  val versionedStore: VersionedStore

  // def shardSet(version: Long) = new ShardSetImpl(versionedStore.versionPath(version), spec)
}

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
    
    path
  }
  
  def deleteVersion(version: Long) {
    versionPath(version).deleteRecursively()
    tokenPath(version).delete()
  }

  def succeedVersion(version: Long) {
    // tokenPath(version).touch()
  }

  def cleanup(versionsToKeep: Int = -1) {
    val keepers = versions.take(versionsToKeep)
    for (p <- root.listDirectories) {
      val v = parseVersion(p)
      if (v.isDefined && !(keepers contains v)) {
        p.deleteRecursively()
      }
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

  def tokenPath(version: Long) = root / (version.toString + versionSuffix)

  def parseVersion(p: FileSystem#Path): Option[Long] = {
    if (p.filename endsWith versionSuffix) {
      Some((p.filename dropRight versionSuffix.length).toLong)
    } else {
      try Some(p.filename.toLong) catch { case e: Exception => None }
    }
  }
  
  /*
    private long validateAndGetVersion(String path) {
        if(!normalizePath(path).getParent().equals(normalizePath(root))) {
            throw new RuntimeException(path + " " + new Path(path).getParent() + " is not part of the versioned store located at " + root);
        }
        Long v = parseVersion(path);
        if(v==null) throw new RuntimeException(path + " is not a valid version");
        return v;
    }

    public Long parseVersion(String path) {
        String name = new Path(path).getName();
        if(name.endsWith(FINISHED_VERSION_SUFFIX)) {
            name = name.substring(0, name.length()-FINISHED_VERSION_SUFFIX.length());
        }
        try {
            return Long.parseLong(name);
        } catch(NumberFormatException e) {
            return null;
        }
    }

    private void createNewFile(String path) throws IOException {
        if(fs instanceof LocalFileSystem)
            new File(path).createNewFile();
        else
            fs.createNewFile(new Path(path));
    }

    private void mkdirs(String path) throws IOException {
        if(fs instanceof LocalFileSystem)
            new File(path).mkdirs();
        else {
            try {
                fs.mkdirs(new Path(path));
            } catch (AccessControlException e) {
                throw new RuntimeException("Root directory doesn't exist, and user doesn't have the permissions " +
                        "to create" + path + ".", e);
            }
        }
    }


    private List<Path> listDir(String dir) throws IOException {
        List<Path> ret = new ArrayList<Path>();
        if(fs instanceof LocalFileSystem) {
            for(File f: new File(dir).listFiles()) {
                ret.add(new Path(f.getAbsolutePath()));
            }
        } else {
            for(FileStatus status: fs.listStatus(new Path(dir))) {
                ret.add(status.getPath());
            }
        }
        return ret;
    }
   */
}

