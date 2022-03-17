package ru.delimobil.fs2hbase.comparator

import org.apache.hadoop.hbase.filter.ByteArrayComparable
import org.apache.hadoop.hbase.util.Bytes

final class FloatComparator(point: Float) extends ByteArrayComparable(Bytes.toBytes(point)) {

  def toByteArray: Array[Byte] = getValue

  def compareTo(value: Array[Byte], offset: Int, length: Int): Int =
    if (length == Bytes.SIZEOF_INT) java.lang.Float.compare(point, Bytes.toFloat(value, offset))
    else throw new IllegalArgumentException(s"Wrong length: $length, expected ${Bytes.SIZEOF_INT}")
}

object FloatComparator {
  def parseFrom(bytes: Array[Byte]): FloatComparator = new FloatComparator(Bytes.toFloat(bytes))
}
