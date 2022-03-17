package ru.delimobil.fs2hbase.comparator

import org.apache.hadoop.hbase.filter.ByteArrayComparable
import org.apache.hadoop.hbase.util.Bytes

final class LongComparator(point: Long) extends ByteArrayComparable(Bytes.toBytes(point)) {

  def toByteArray: Array[Byte] = getValue

  def compareTo(value: Array[Byte], offset: Int, length: Int): Int =
    java.lang.Long.compare(point, Bytes.toLong(value, offset, length))
}

object LongComparator {
  def parseFrom(bytes: Array[Byte]): LongComparator = new LongComparator(Bytes.toLong(bytes))
}
