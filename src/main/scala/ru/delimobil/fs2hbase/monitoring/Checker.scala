package ru.delimobil.fs2hbase.monitoring

trait Checker[F[_]] {
  def check: F[Boolean]
}
