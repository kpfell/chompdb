package chompdb.server

import scala.util.hashing.MurmurHash3

object Hashing {
  def hash(str: String): Int = MurmurHash3.stringHash(str)
}