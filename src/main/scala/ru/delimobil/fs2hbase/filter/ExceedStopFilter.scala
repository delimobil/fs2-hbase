package ru.delimobil.fs2hbase.filter

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellComparator
import org.apache.hadoop.hbase.filter.FilterBase

final class ExceedStopFilter(stopRowKey: Array[Byte]) extends FilterBase {

  private var cnt = 0

  override def filterRowKey(firstRowCell: Cell): Boolean =
    filterAllRemaining() || {
      val cmp = CellComparator.getInstance.compareRows(firstRowCell, stopRowKey, 0, stopRowKey.length)
      val bool = if(reversed) cmp <= 0 else cmp >= 0
      if (bool) cnt += 1
      done
    }

  override def filterAllRemaining(): Boolean = done

  override def toByteArray: Array[Byte] = stopRowKey

  private def done: Boolean = cnt > 1
}

object ExceedStopFilter {
  def parseFrom(bytes: Array[Byte]): ExceedStopFilter = new ExceedStopFilter(bytes)
}
