package ru.delimobil.fs2hbase.comparator

import org.apache.hadoop.hbase.filter.ByteArrayComparable
import org.apache.hadoop.hbase.util.Bytes

final class IntComparator(point: Int) extends ByteArrayComparable(Bytes.toBytes(point)) {

  def toByteArray: Array[Byte] = getValue

  def compareTo(value: Array[Byte], offset: Int, length: Int): Int = {
    java.lang.Integer.compare(point, Bytes.toInt(value, offset, length))
  }
}

object IntComparator {
  def parseFrom(bytes: Array[Byte]): IntComparator = new IntComparator(Bytes.toInt(bytes))
}
