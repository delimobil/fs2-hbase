package ru.delimobil.fs2hbase.api

import fs2.Stream
import org.apache.hadoop.hbase.client
import ru.delimobil.fs2hbase.codec.Decoder
import ru.delimobil.fs2hbase.codec.Encoder

trait Table[F[_]] {

  def put[V](value: V)(implicit encoder: Encoder[V]): F[Unit]

  def put[V](values: List[V])(implicit encoder: Encoder[V]): F[Unit]

  def get[V](request: client.Get)(implicit decoder: Decoder[V]): F[Option[V]]

  def getScanner[V](scan: client.Scan)(implicit decoder: Decoder[V]): Stream[F, V]

  def delay[V](f: client.Table => V): F[V]
}
