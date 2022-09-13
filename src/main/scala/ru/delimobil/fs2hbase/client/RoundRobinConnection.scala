package ru.delimobil.fs2hbase.client

import cats.Monad
import cats.effect.Resource
import cats.effect.std.Random
import ru.delimobil.fs2hbase.api.Admin
import ru.delimobil.fs2hbase.api.Connection
import ru.delimobil.fs2hbase.api.Table
import ru.delimobil.fs2hbase.model.HBaseClientTableName

final class RoundRobinConnection[F[_]: Monad: Random](connections: Vector[Connection[F]]) extends Connection[F] {

  def getAdmin: Resource[F, Admin[F]] =
    Resource.eval(Random[F].nextIntBounded(connections.length)).flatMap(i => connections(i).getAdmin)

  def getTable(tableName: HBaseClientTableName): Resource[F, Table[F]] =
    Resource.eval(Random[F].nextIntBounded(connections.length)).flatMap(i => connections(i).getTable(tableName))
}
