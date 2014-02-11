package chompdb

import java.util.Properties
import scala.collection.JavaConverters._
import f1lesystem.FileSystem

object Catalog {
  def apply(name: String, dir: FileSystem#Dir) = new Catalog(name, dir)
}

class Catalog(
  val name: String,
  val base: FileSystem#Dir
) {
  def database(name: String) = new Database(this, name)

  val dir = base /+ name

  override def equals(other: Any) = other match {
    case c: Catalog => (c.name == name)
  }

  override def toString = s"Catalog($name, $base)"
}