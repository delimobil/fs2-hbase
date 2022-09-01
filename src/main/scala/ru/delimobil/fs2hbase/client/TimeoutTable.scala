package ru.delimobil.fs2hbase.client

import cats.effect.kernel.Temporal
import cats.effect.syntax.temporal._
import cats.syntax.functor._
import fs2.Pull
import fs2.Stream
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Scan
import ru.delimobil.fs2hbase.api.Table
import ru.delimobil.fs2hbase.codec.Decoder
import ru.delimobil.fs2hbase.codec.Encoder

import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

final class TimeoutTable[F[_]: Temporal](
    delegatee: Table[F],
    timeout: FiniteDuration
) extends Table[F] {

  def put[V](value: V)(implicit encoder: Encoder[V]): F[Unit] =
    delegatee.put(value).timeoutAndForget(timeout)

  def put[V](values: List[V])(implicit encoder: Encoder[V]): F[Unit] =
    delegatee.put(values).timeoutAndForget(timeout)

  def get[V](request: Get)(implicit decoder: Decoder[V]): F[Option[V]] =
    delegatee.get(request).timeoutAndForget(timeout)

  def getScannerAction[V](scan: Scan, chunkSize: Int)(implicit decoder: Decoder[V]): F[Stream[F, V]] =
    delegatee.getScannerAction(scan).timeoutAndForget(timeout).map { stream =>
      def go(timedPull: Pull.Timed[F, V]): Pull[F, V, Unit] =
        timedPull.timeout(timeout) >>
          timedPull.uncons.flatMap {
            case Some((Right(elems), next)) => Pull.output(elems) >> go(next)
            case Some((Left(_), _)) => Pull.raiseError(new TimeoutException(timeout.toString))
            case None               => Pull.done
          }

      stream.pull.timed(go).stream
    }

  def getScanner[V](scan: Scan, chunkSize: Int)(implicit decoder: Decoder[V]): Stream[F, V] =
    Stream.force(getScannerAction(scan))
}
