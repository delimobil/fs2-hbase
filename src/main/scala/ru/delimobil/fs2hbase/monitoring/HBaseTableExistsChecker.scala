package ru.delimobil.fs2hbase.monitoring

import cats.effect.MonadCancelThrow
import cats.effect.Resource
import ru.delimobil.fs2hbase.api.Admin
import ru.delimobil.fs2hbase.api.Connection
import ru.delimobil.fs2hbase.model.HBaseClientTableName

private abstract class HBaseTableExistsChecker[F[_]: MonadCancelThrow](
    tableName: HBaseClientTableName
) extends Checker[F] {

  protected def getAdmin: Resource[F, Admin[F]]

  def check: F[Boolean] = getAdmin.use(_.tableExists(tableName))
}

object HBaseTableExistsChecker {

  def meta: Either[IllegalArgumentException, HBaseClientTableName] =
    HBaseClientTableName("hbase:meta")

  def namespace: Either[IllegalArgumentException, HBaseClientTableName] =
    HBaseClientTableName("hbase:namespace")

  def apply[F[_]: MonadCancelThrow](
      connection: Connection[F],
      tableName: HBaseClientTableName
  ): Checker[F] =
    new HBaseTableExistsChecker[F](tableName) {
      def getAdmin: Resource[F, Admin[F]] = connection.getAdmin
    }

  def apply[F[_]: MonadCancelThrow](
      admin: Admin[F],
      tableName: HBaseClientTableName
  ): Checker[F] =
    new HBaseTableExistsChecker[F](tableName) {
      def getAdmin: Resource[F, Admin[F]] = Resource.pure(admin)
    }
}
