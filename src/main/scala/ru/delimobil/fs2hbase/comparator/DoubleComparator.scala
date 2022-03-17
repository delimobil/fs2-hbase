package ru.delimobil.fs2hbase.comparator

import org.apache.hadoop.hbase.filter.ByteArrayComparable
import org.apache.hadoop.hbase.util.Bytes

final class DoubleComparator(point: Double) extends ByteArrayComparable(Bytes.toBytes(point)) {

  def toByteArray: Array[Byte] = getValue

  def compareTo(value: Array[Byte], offset: Int, length: Int): Int =
    if (length == Bytes.SIZEOF_LONG) java.lang.Double.compare(point, Bytes.toDouble(value, offset))
    else throw new IllegalArgumentException(s"Wrong length: $length, expected ${Bytes.SIZEOF_LONG}")
}

object DoubleComparator {
  def parseFrom(bytes: Array[Byte]): DoubleComparator = new DoubleComparator(Bytes.toDouble(bytes))
}
