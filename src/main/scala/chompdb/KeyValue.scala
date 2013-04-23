package chompdb

object KeyValue {
  type Key = Array[Byte]
  type Value = Array[Byte]
}

import KeyValue._

case class KeyValue(key: Key, value: Value) {
  override def toString = "(" + String.valueOf(key) + " -> " + String.valueOf(value) + ")"
}
