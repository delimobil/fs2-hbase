package ru.delimobil.fs2hbase.client

import cats.effect.Sync
import cats.effect.std.Semaphore
import org.apache.hadoop.hbase.client
import ru.delimobil.fs2hbase.api.Admin
import ru.delimobil.fs2hbase.model.HBaseClientTableDescriptor
import ru.delimobil.fs2hbase.model.HBaseClientTableName

final class HBaseClientAdmin[F[_]: Sync](
    semaphore: Semaphore[F],
    admin: client.Admin
) extends Admin[F] {

  def delay[V](f: client.Admin => V): F[V] =
    withPermit(f(admin))

  def tableExists(tableName: HBaseClientTableName): F[Boolean] =
    withPermit(admin.tableExists(tableName.raw))

  def createTable(hBaseTable: HBaseClientTableDescriptor): F[Unit] =
    withPermit(admin.createTable(hBaseTable.raw))

  def deleteTable(hBaseTable: HBaseClientTableName): F[Unit] =
    withPermit(admin.deleteTable(hBaseTable.raw))

  def disableTable(hBaseTable: HBaseClientTableName): F[Unit] =
    withPermit(admin.disableTable(hBaseTable.raw))

  def truncateTable(hBaseTable: HBaseClientTableName, preserveSplits: Boolean): F[Unit] =
    withPermit(admin.truncateTable(hBaseTable.raw, preserveSplits))

  private def withPermit[V](thunk: => V): F[V] =
    semaphore.permit.use(_ => Sync[F].blocking(thunk))
}
