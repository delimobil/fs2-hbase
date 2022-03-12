package ru.delimobil.fs2hbase.model

import cats.data.NonEmptyList
import ru.delimobil.fs2hbase.model.Fs2HBaseConfig._

case class Fs2HBaseConfig(
    zooKeeperEnsemble: NonEmptyList[Host],
    port: Port,
    columnMaxVersion: Long = 1, // TODO: looks like it should be defined in site.xml
    scannerIterationLength: Long = 2_097_152, // TODO: looks like it should be defined in site.xml
    retries: Int = 31,
    rpcTimeout: Int = 60_000,
    scanTimeout: Int = 60_000,
    cellsPerHeartbeat: Int = 10_000
)

object Fs2HBaseConfig {

  type Host = String

  type Port = Int
}
