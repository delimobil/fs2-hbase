package ru.delimobil.fs2hbase.codec

import org.apache.hadoop.hbase.client

trait Decoder[V] {
  def decode(result: client.Result): V
}
