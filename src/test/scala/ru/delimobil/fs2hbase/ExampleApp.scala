package ru.delimobil.fs2hbase

import cats.data.NonEmptyList
import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.syntax.semigroupal._
import org.apache.hadoop.hbase.client
import org.apache.hadoop.hbase.util.Bytes
import ru.delimobil.fs2hbase.Example._
import ru.delimobil.fs2hbase.model.Fs2HBaseConfig

import java.time.LocalDateTime
import scala.concurrent.duration._

object ExampleApp extends IOApp {

  // test:runMain fs2hbase.ExampleApp 127.0.0.1 2181
  override def run(args: List[String]): IO[ExitCode] =
    args match {
      case host :: port :: Nil => doRun(host, port.toInt)
      case _ => IO.println("test:runMain fs2hbase.ExampleApp 127.0.0.1 2181").as(ExitCode.Error)
    }

  private def doRun(host: String, port: Int): IO[ExitCode] = {
    val config = Fs2HBaseConfig(NonEmptyList.one(host), port)

    val zdt1 = LocalDateTime.of(2020, 9, 1, 10, 0, 5).atZone(zone)
    val zdt2 = LocalDateTime.of(2020, 9, 1, 10, 0, 10).atZone(zone)
    val zdt3 = LocalDateTime.of(2020, 9, 1, 10, 0, 15).atZone(zone)
    val zdt4 = LocalDateTime.of(2020, 9, 1, 10, 0, 20).atZone(zone)
    val zdt5 = LocalDateTime.of(2020, 9, 1, 10, 0, 25).atZone(zone)

    val speed10 = Speed(10, zdt1)
    val speed20 = Speed(11, zdt2)
    val speed21 = Speed(12, zdt2)
    val speed30 = Speed(13, zdt3)
    val speed31 = Speed(16, zdt3)
    val speed40 = Speed(20, zdt4)
    val speed50 = Speed(20, zdt5)

    ConnectionResource
      .makeTimeout[IO](config, 10.seconds)
      .flatMap(c => c.getAdmin.product(c.getTable(speedTable.tableName)))
      .use { case (admin, table) =>
        val scan = new client.Scan()
        scan.withStartRow(Bytes.toBytes(zdt2.toEpochSecond))
        scan.withStopRow(Bytes.toBytes(zdt4.toEpochSecond))

        for {
          isTableExists <- admin.tableExists(speedTable.tableName)
          expectedMsg1 = s"${!isTableExists}: isTableExists is expected to be false"
          _ <- IO.println(expectedMsg1 + s", actual: $isTableExists")
          _ <- if (isTableExists) IO.unit else admin.createTable(speedTable)
          tableShouldBePresent <- admin.tableExists(speedTable.tableName)
          expectedMsg2 = s"$tableShouldBePresent: tableShouldBePresent is expected to be true"
          _ <- IO.println(expectedMsg2 + s"actual: $tableShouldBePresent")
          _ <- IO.println(s"tableName: ${speedTable.tableName}")
          _ <- table.put(List(speed10, speed20, speed21))
          _ <- table.put(speed30)
          _ <- table.put(speed31)
          _ <- table.put(List(speed40, speed50))
          speeds <- table.getScanner(scan).compile.toList
          bool = List(speed21, speed31) == speeds
          expectedMsg3 = s"$bool: speeds are expected to be List($speed21, $speed31)"
          _ <- IO.println(expectedMsg3 + s", actual: $speeds")
          _ <- admin.disableTable(speedTable.tableName)
          _ <- admin.deleteTable(speedTable.tableName)
        } yield ExitCode.Success
      }
  }
}
