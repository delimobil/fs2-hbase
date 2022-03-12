package ru.delimobil.fs2hbase.model

import cats.data.NonEmptyList
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder
import org.apache.hadoop.hbase.client.TableDescriptor
import org.apache.hadoop.hbase.client.TableDescriptorBuilder
import ru.delimobil.fs2hbase.model.HBaseClientTableDescriptor.HBaseColumnFamily

case class HBaseClientTableDescriptor private (
    tableName: HBaseClientTableName,
    columnFamilies: NonEmptyList[HBaseColumnFamily]
) {

  def raw: TableDescriptor = {
    val builder = TableDescriptorBuilder.newBuilder(tableName.raw)
    columnFamilies.toList.foreach { family =>
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family.prefix))
    }
    builder.build()
  }
}

object HBaseClientTableDescriptor {

  case class HBaseColumnFamily(prefix: String) extends AnyVal

  def apply[V](
      name: String,
      columnFamilies: NonEmptyList[HBaseColumnFamily]
  ): Either[IllegalArgumentException, HBaseClientTableDescriptor] =
    HBaseClientTableName(name).map(new HBaseClientTableDescriptor(_, columnFamilies))

  private def apply(
    tableName: HBaseClientTableName,
    columnFamilies: NonEmptyList[HBaseColumnFamily]
  ) = new HBaseClientTableDescriptor(tableName, columnFamilies)
}
