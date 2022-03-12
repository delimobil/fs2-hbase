package ru.delimobil.fs2hbase

import cats.data.NonEmptyList
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import ru.delimobil.fs2hbase.codec.Decoder
import ru.delimobil.fs2hbase.codec.Encoder
import ru.delimobil.fs2hbase.model.HBaseClientTableDescriptor
import ru.delimobil.fs2hbase.model.HBaseClientTableDescriptor.HBaseColumnFamily

import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

object Example {

  sealed trait Timed {
    def date: ZonedDateTime
    def bytes: Array[Byte]
  }

  trait IntTimed extends Timed {
    protected def value: Int
    final def bytes: Array[Byte] = Bytes.toBytes(value)
  }

  case class Speed(value: Int, date: ZonedDateTime) extends IntTimed

  val zone: ZoneId = ZoneId.systemDefault()

  private val columnFamily = HBaseColumnFamily("cf")
  private val columnFamilyBytes = Bytes.toBytes(columnFamily.prefix)
  private val columnBytes = Bytes.toBytes("c")
  val speedTable: HBaseClientTableDescriptor =
    HBaseClientTableDescriptor[Speed]("speed", NonEmptyList.one(columnFamily)).getOrElse(???)

  implicit def deriveHBaseEncoder[V <: Timed]: Encoder[V] =
    new Encoder[V] {
      def encode(value: V): Put = {
        val put = new Put(Bytes.toBytes(value.date.toEpochSecond))
        put.addColumn(columnFamilyBytes, columnBytes, value.bytes)
      }
    }

  implicit val speedDecoder: Decoder[Speed] =
    new Decoder[Speed] {
      def decode(result: Result): Speed = {
        val epochSeconds = Bytes.toLong(result.getRow)
        val speed = Bytes.toInt(result.getValue(columnFamilyBytes, columnBytes))
        Speed(speed, Instant.ofEpochSecond(epochSeconds).atZone(zone))
      }
    }
}
