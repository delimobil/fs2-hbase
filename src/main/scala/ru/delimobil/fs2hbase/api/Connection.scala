package ru.delimobil.fs2hbase.api

import cats.effect.Resource
import ru.delimobil.fs2hbase.model.HBaseClientTableName

trait Connection[F[_]] {

  def getAdmin: Resource[F, Admin[F]]

  def getTable(tableName: HBaseClientTableName): Resource[F, Table[F]]

  def isClosed: F[Boolean]
}
