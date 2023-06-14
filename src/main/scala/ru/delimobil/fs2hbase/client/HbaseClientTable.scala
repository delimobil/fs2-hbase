package ru.delimobil.fs2hbase.client

import cats.effect.Resource
import cats.effect.Sync
import cats.effect.std.Semaphore
import cats.syntax.functor._
import cats.syntax.option._
import fs2.Stream
import org.apache.hadoop.hbase.client
import org.apache.hadoop.hbase.client.Get
import ru.delimobil.fs2hbase.api.Table
import ru.delimobil.fs2hbase.codec.Decoder
import ru.delimobil.fs2hbase.codec.Encoder

import scala.jdk.CollectionConverters._

private[fs2hbase] final class HbaseClientTable[F[_]: Sync](
    semaphore: Semaphore[F],
    table: client.Table,
    chunkSize: Int
) extends Table[F] {

  def put[V](value: V)(implicit encoder: Encoder[V]): F[Unit] =
    put(List(value))

  def put[V](values: List[V])(implicit encoder: Encoder[V]): F[Unit] =
    withPermit(table.put(values.map(encoder.encode).asJava))

  def get[V](request: Get)(implicit decoder: Decoder[V]): F[Option[V]] =
    withPermit(table.get(request)).map { result =>
      if (result.isEmpty) none
      else decoder.decode(result).some
    }

  def getScanner[V](scan: client.Scan)(implicit decoder: Decoder[V]): Stream[F, V] = {
    val resource = Resource.fromAutoCloseable(withPermit(table.getScanner(scan)))
    Stream.resource(resource).flatMap { resultScanner =>
      val iterator = resultScanner.iterator().asScala
      val stream = Stream.fromBlockingIterator(iterator, chunkSize)
      stream.map(result => decoder.decode(result))
    }
  }

  def delay[V](f: client.Table => V): F[V] =
    withPermit(f(table))

  private def withPermit[V](thunk: => V): F[V] =
    semaphore.permit.use(_ => Sync[F].blocking(thunk))
}
