package ru.delimobil.fs2hbase.api

import ru.delimobil.fs2hbase.model.HBaseClientTableDescriptor
import ru.delimobil.fs2hbase.model.HBaseClientTableName
import org.apache.hadoop.hbase.client

trait Admin[F[_]] {

  def delay[V](f: client.Admin => V): F[V]

  def tableExists(tableName: HBaseClientTableName): F[Boolean]

  def createTable(tableDefinition: HBaseClientTableDescriptor): F[Unit]

  def deleteTable(tableName: HBaseClientTableName): F[Unit]

  def disableTable(tableName: HBaseClientTableName): F[Unit]

  def truncateTable(tableName: HBaseClientTableName, preserveSplits: Boolean): F[Unit]
}
