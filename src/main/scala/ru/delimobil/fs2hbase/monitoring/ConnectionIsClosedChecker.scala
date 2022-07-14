package ru.delimobil.fs2hbase.monitoring

import ru.delimobil.fs2hbase.api.Connection

final class ConnectionIsClosedChecker[F[_]](connection: Connection[F]) extends Checker[F] {
  def check: F[Boolean] = connection.isClosed
}
