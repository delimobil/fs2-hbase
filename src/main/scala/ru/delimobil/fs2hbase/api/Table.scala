package ru.delimobil.fs2hbase.api

import fs2.Stream
import org.apache.hadoop.hbase.client
import ru.delimobil.fs2hbase.codec.Decoder
import ru.delimobil.fs2hbase.codec.Encoder

trait Table[F[_]] {

  def put[V](value: V)(implicit encoder: Encoder[V]): F[Unit]

  def put[V](values: List[V])(implicit encoder: Encoder[V]): F[Unit]

  def getScannerAction[V](scan: client.Scan)(implicit decoder: Decoder[V]): F[Stream[F, V]]

  def getScanner[V](scan: client.Scan)(implicit decoder: Decoder[V]): Stream[F, V]
}
