package ru.delimobil.fs2hbase.client

import cats.effect.Sync
import cats.effect.std.Semaphore
import cats.syntax.flatMap._
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

  def getScannerAction[V](scan: client.Scan)(implicit decoder: Decoder[V]): F[Stream[F, V]] =
    withPermit(table.getScanner(scan)).flatMap { resultScanner =>
      val iterator = resultScanner.iterator().asScala
      val stream = Stream.fromBlockingIterator(iterator, getBatch(scan))
      triggerUpload(iterator).as(stream.map(result => decoder.decode(result)))
    }

  def getScanner[V](scan: client.Scan)(implicit decoder: Decoder[V]): Stream[F, V] = {
    val action = withPermit(table.getScanner(scan)).map { resultScanner =>
      val iterator = resultScanner.iterator().asScala
      val stream = Stream.fromBlockingIterator(iterator, getBatch(scan))
      stream.map(result => decoder.decode(result))
    }
    Stream.force(action)
  }

  private def getBatch(scan: client.Scan): Int = {
    val batch = scan.getBatch
    if (batch == -1) chunkSize else batch
  }

  private def withPermit[V](thunk: => V): F[V] =
    semaphore.permit.use(_ => Sync[F].blocking(thunk))

  private def triggerUpload[V](iterator: Iterator[V]): F[Unit] =
    Sync[F].blocking(iterator.hasNext).void
}
