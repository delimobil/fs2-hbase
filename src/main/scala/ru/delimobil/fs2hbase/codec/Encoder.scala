package ru.delimobil.fs2hbase.codec

import org.apache.hadoop.hbase.client

trait Encoder[V] {
  def encode(value: V): client.Put
}
