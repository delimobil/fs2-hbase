package ru.delimobil.fs2hbase.comparator

import org.apache.hadoop.hbase.filter.ByteArrayComparable
import org.apache.hadoop.hbase.util.Bytes

final class BooleanComparator(point: Boolean) extends ByteArrayComparable(Bytes.toBytes(point)) {

  def toByteArray: Array[Byte] = getValue

  def compareTo(value: Array[Byte], offset: Int, length: Int): Int =
    java.lang.Boolean.compare(point, Bytes.toBoolean(value.slice(offset, length)))
}

object BooleanComparator {
  def parseFrom(bytes: Array[Byte]): BooleanComparator = new BooleanComparator(Bytes.toBoolean(bytes))
}
