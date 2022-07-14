package ru.delimobil.fs2hbase.client

import cats.effect.Resource
import cats.effect.kernel.Temporal
import cats.effect.syntax.temporal._
import ru.delimobil.fs2hbase.api.Admin
import ru.delimobil.fs2hbase.api.Connection
import ru.delimobil.fs2hbase.api.Table
import ru.delimobil.fs2hbase.model.HBaseClientTableName

import scala.concurrent.duration.FiniteDuration

final class TimeoutConnection[F[_]: Temporal](
    delegatee: Connection[F],
    timeout: FiniteDuration
) extends Connection[F] {

  def getAdmin: Resource[F, Admin[F]] =
    delegatee.getAdmin.map(new TimeoutAdmin(_, timeout)).timeoutAndForget(timeout)

  def getTable(tableName: HBaseClientTableName): Resource[F, Table[F]] =
    delegatee.getTable(tableName).map(new TimeoutTable(_, timeout)).timeoutAndForget(timeout)

  def isClosed: F[Boolean] =
    delegatee.isClosed.timeoutAndForget(timeout)
}
