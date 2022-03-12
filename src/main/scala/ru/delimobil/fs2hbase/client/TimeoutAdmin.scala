package ru.delimobil.fs2hbase.client

import cats.effect.kernel.Temporal
import cats.effect.syntax.temporal._
import ru.delimobil.fs2hbase.api.Admin
import ru.delimobil.fs2hbase.model.HBaseClientTableDescriptor
import ru.delimobil.fs2hbase.model.HBaseClientTableName

import scala.concurrent.duration.FiniteDuration

final class TimeoutAdmin[F[_]: Temporal](
    delegatee: Admin[F],
    timeout: FiniteDuration
) extends Admin[F] {

  def tableExists(tableName: HBaseClientTableName): F[Boolean] =
    delegatee.tableExists(tableName).timeoutAndForget(timeout)

  def createTable(tableDefinition: HBaseClientTableDescriptor): F[Unit] =
    delegatee.createTable(tableDefinition).timeoutAndForget(timeout)

  def deleteTable(tableName: HBaseClientTableName): F[Unit] =
    delegatee.deleteTable(tableName).timeoutAndForget(timeout)

  def disableTable(tableName: HBaseClientTableName): F[Unit] =
    delegatee.disableTable(tableName).timeoutAndForget(timeout)

  def truncateTable(tableName: HBaseClientTableName, preserveSplits: Boolean): F[Unit] =
    delegatee.truncateTable(tableName, preserveSplits).timeoutAndForget(timeout)
}
