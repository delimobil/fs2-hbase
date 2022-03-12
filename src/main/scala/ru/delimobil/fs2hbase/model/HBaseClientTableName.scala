package ru.delimobil.fs2hbase.model

import cats.syntax.either._
import org.apache.hadoop.hbase

case class HBaseClientTableName private (raw: hbase.TableName) extends AnyVal

object HBaseClientTableName {

  def apply(name: String): Either[IllegalArgumentException, HBaseClientTableName] =
    Either.catchOnly[IllegalArgumentException] {
      new HBaseClientTableName(hbase.TableName.valueOf(name))
    }

  private def apply(raw: hbase.TableName): HBaseClientTableName =
    new HBaseClientTableName(raw)
}
