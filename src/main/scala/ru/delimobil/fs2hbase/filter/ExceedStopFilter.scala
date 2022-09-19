package ru.delimobil.fs2hbase.filter

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellComparator
import org.apache.hadoop.hbase.filter.FilterBase

final class ExceedStopFilter(stopRowKey: Array[Byte]) extends FilterBase {

  var done = false

  override def filterRowKey(firstRowCell: Cell): Boolean = {
    val cmp = CellComparator.getInstance.compareRows(firstRowCell, stopRowKey, 0, stopRowKey.length)
    done = if (reversed) cmp <= 0 else cmp >= 0
    false
  }

  override def filterAllRemaining(): Boolean = done

  override def toByteArray: Array[Byte] = stopRowKey
}

object ExceedStopFilter {
  def parseFrom(bytes: Array[Byte]): ExceedStopFilter = new ExceedStopFilter(bytes)
}
