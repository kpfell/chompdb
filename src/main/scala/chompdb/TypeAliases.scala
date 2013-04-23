package chompdb

object TypeAliases {
  type KeyValue = (Key, Value)

  type Key = Array[Byte]
  type Value = Array[Byte]
}