package chompdb.server

import java.nio.ByteBuffer

@serializable
trait Mapper[T, U] {
  /** Typically named `flatMap` but we're trying to be faithful
   *  to map-reduce naming conventions.
   */
  def map(t: T): Seq[U]
}

@serializable
trait Reducer[T] {
  def reduce(t1: T, t2: T): T
}

@serializable
trait MapReduce[T, U] extends Mapper[T, U] with Reducer[U]

/** A remoting key-value store exposing map-reduce push-down processing */
trait SlapChop {
  def apply[T](keys: Seq[Long], mapReduce: MapReduce[ByteBuffer, T]): T
}


/*
layering:

app-specific interfaces, remoting
processor
pure key-value store


S3 format:

root = bucket:base/path/
  versions => contains known versions + metadata

  files stored in hex-encoded hierarchy

  bucket:base/path/12345/
*/


/**


*/
