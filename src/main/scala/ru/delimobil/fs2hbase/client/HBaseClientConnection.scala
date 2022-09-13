package ru.delimobil.fs2hbase.client

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Sync
import cats.effect.std.Semaphore
import org.apache.hadoop.hbase
import ru.delimobil.fs2hbase.api.Admin
import ru.delimobil.fs2hbase.api.Connection
import ru.delimobil.fs2hbase.api.Table
import ru.delimobil.fs2hbase.model.HBaseClientTableName

final class HBaseClientConnection[F[_]: Async](
    connection: hbase.client.Connection
) extends Connection[F] {

  def getAdmin: Resource[F, Admin[F]] =
    create(connection.getAdmin)((semaphore, admin) => new HBaseClientAdmin[F](semaphore, admin))

  def getTable(tableName: HBaseClientTableName): Resource[F, Table[F]] =
    create(connection.getTable(tableName.raw)) { (semaphore, table) =>
      new HbaseClientTable(semaphore, table)
    }

  private def create[A <: AutoCloseable, V](
      thunk: => A
  )(f: (Semaphore[F], A) => V): Resource[F, V] =
    Resource.fromAutoCloseable(Sync[F].blocking(thunk)).flatMap { a =>
      Resource.eval(Semaphore[F](1)).map(f(_, a))
    }
}
