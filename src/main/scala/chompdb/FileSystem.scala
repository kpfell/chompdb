package chompdb

import java.io.{InputStream, Reader}
import java.nio.ByteBuffer
import java.nio.charset.Charset

object FileSystem {
  val UTF8 = Charset.forName("UTF-8")
}

trait FileSystem {
  type FILE <: File
  type DIR  <: Dir
  
  def root: DIR

  def parseFile(path: String): FILE
  def parseDirectory(path: String): DIR
  
  trait Path {
    def filename: String

    def fullpath: String

    /** Return file extension, e.g. "foo.bar" => "bar" or "package.tar.gz" => "tar.gz" */
    def extension: String = {
      val dot = filename.indexOf(".")
      if (dot == -1) "" 
      else filename.substring(dot + 1, filename.length)
    }
    
    /** Return filename without extension, e.g. "foo.bar" => "foo" or "package.tar.gz" => "package" */
    def basename: String = {
      val dot = filename.indexOf(".")
      if (dot == -1) filename 
      else filename.substring(0, dot)
    }
    
    /** Returns true if file or directory exists */
    def exists: Boolean

    /** Delete the file or directory. */
    def delete(): Unit
    
    override def equals(x: Any) = x match {
      case other: Path => other.fullpath == this.fullpath
      case _ => false
    }

    override def hashCode = fullpath.hashCode
    
    override def toString = fullpath
  }
  
  trait Dir extends Path {
    def parent: Option[DIR]
      
    def listFiles: Seq[FILE]
    def listDirectories: Seq[DIR]
  
    def /(filename: String): FILE 
    def /+(filename: String): DIR
    
    def mkdir(): Unit

    def deleteRecursively(): Unit

    override def fullpath = (parent map (_.fullpath) getOrElse "") + "/" + filename
  }
  
  trait File extends Path {
    def parent: DIR

    def touch(): Unit

    def readAsByteBuffer(): ByteBuffer
    def readAsInputStream(): InputStream
    def readAsString(charSet: Charset = FileSystem.UTF8): String
    def readAsReader(): Reader
    
    def write(data: ByteBuffer): Unit
    def write(str: String, charSet: Charset = chompdb.FileSystem.UTF8): Unit
    def write(stream: InputStream): Unit
    def write(reader: Reader): Unit

    override def fullpath = parent.fullpath + "/" + filename
  }
}


class LocalFileSystem extends FileSystem {
  type PATH = LocalPath
  type FILE = LocalFile
  type DIR = LocalDir

  override def root = new LocalDir(None, "")
  
  def parseFile(path: String): FILE = {
    val file = new java.io.File(path)
    val parent = if (file.getParentFile == null) root else parseDirectory(file.getParentFile.getCanonicalPath)
    new LocalFile(parent, file.getName)
  }
  
  def parseDirectory(path: String): DIR = {
    val file = new java.io.File(path)
    new LocalDir(Option(file.getParentFile) map (_.getCanonicalPath) map parseDirectory, file.getName)
  }
  
  trait LocalPath extends Path {
    override def exists = file.exists
    
    override def delete() { file.delete() }
  
    private[chompdb] def file = new java.io.File(fullpath)
  }
  
  class LocalFile(override val parent: DIR, override val filename: String ) extends LocalPath with File {
    import java.io._
    import java.nio.charset._
    
    override def readAsByteBuffer(): java.nio.ByteBuffer = {
      val f = new java.io.RandomAccessFile(file, "r")
      try {
        val size = f.length().toInt // WARNING: Does not support > 4GB files
        val buf = ByteBuffer.allocate(size.toInt)
        val c = f.getChannel
        var pos = 0
        while (pos < size) {
          val n = c.read(buf)
          if (n == -1) return buf
          pos += n
        }
        buf.position(0) // ready to be read
        buf
      } finally {
        f.close()
      }
    } 
    
    override def readAsInputStream(): java.io.InputStream = {
      new java.io.FileInputStream(file)
    }
    
    override def readAsReader(): java.io.Reader = {
      new java.io.BufferedReader(new java.io.FileReader(file))
    }
    
    override def readAsString(charSet: Charset = chompdb.FileSystem.UTF8) = {
      val buf = readAsByteBuffer()
      charSet.newDecoder().decode(buf).toString
    }
    
    override def touch() { 
      new java.io.FileOutputStream(file).close()
    }

    override def write(reader: java.io.Reader): Unit = {
      val buf = new Array[Char](256 * 1024)
      val writer = new FileWriter(file)
      try {
        while (true) {
          val n = reader.read(buf)
          if (n == -1) return
          writer.write(buf, 0, n)
        }
      } finally {
        writer.close()
      }
    }
    
    def write(stream: java.io.InputStream): Unit = {
      val buf = new Array[Byte](256 * 1024)
      val out = new FileOutputStream(file)
      try {
        while (true) {
          val n = stream.read(buf)
          if (n == -1) return
          out.write(buf, 0, n)
        }
      } finally {
        out.close()
      }
    }

    def write(str: String, charSet: Charset = chompdb.FileSystem.UTF8): Unit = {
      write(charSet.encode(str))
    }
    
    def write(data: java.nio.ByteBuffer): Unit = {
      val f = new java.io.RandomAccessFile(file, "rw")
      try {
        f.setLength(0) // truncate if needed
        val c = f.getChannel
        while (data.hasRemaining) {
          c.write(data)
        }
      } finally {
        f.close()
      }
    } 
  }
  
  class LocalDir(
    override val parent: Option[DIR], 
    override val filename: String
  ) extends LocalPath with Dir {
    override def listFiles: Seq[FILE] = {
      val files = file.listFiles
      if (files == null) return Seq.empty
      files filter (_.isFile) map { f => new LocalFile(this, f.getName) }
    }
    
    override def listDirectories: Seq[DIR] = {
      val files = file.listFiles
      if (files == null) return Seq.empty
      files filter (_.isDirectory) map { d => new LocalDir(Some(this), d.getName) }
    }
  
    override def /(filename: String): FILE = new LocalFile(this, filename)
    
    override def /+(filename: String): DIR = new LocalDir(Some(this), filename) 

    override def mkdir() { file.mkdirs() }
    
    override def deleteRecursively() {
      if (!exists) return
      for (d <- listDirectories) { d.deleteRecursively() }
      for (f <- listFiles) { f.delete() }
      delete()
    }
  }
}
