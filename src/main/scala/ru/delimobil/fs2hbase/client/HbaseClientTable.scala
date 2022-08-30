package ru.delimobil.fs2hbase.client

import cats.effect.Sync
import cats.effect.std.Semaphore
import cats.syntax.functor._
import fs2.Stream
import org.apache.hadoop.hbase.client
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

  def getScannerAction[V](scan: client.Scan)(implicit decoder: Decoder[V]): F[Stream[F, V]] =
    withPermit(table.getScanner(scan)).map { resultScanner =>
      val stream = Stream.fromBlockingIterator(resultScanner.iterator().asScala, chunkSize)
      stream.map(result => decoder.decode(result))
    }

  def getScanner[V](scan: client.Scan)(implicit decoder: Decoder[V]): Stream[F, V] =
    Stream.force(getScannerAction(scan))

  private def withPermit[V](thunk: => V): F[V] =
    semaphore.permit.use(_ => Sync[F].blocking(thunk))
}
