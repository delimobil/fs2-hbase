package ru.delimobil.fs2hbase

import cats.effect.Resource
import cats.effect.Sync
import cats.effect.kernel.Async
import cats.effect.syntax.temporal._
import org.apache.hadoop.conf
import org.apache.hadoop.hbase
import org.apache.hadoop.hbase.HConstants
import ru.delimobil.fs2hbase.client.HBaseClientConnection
import ru.delimobil.fs2hbase.client.TimeoutConnection
import ru.delimobil.fs2hbase.model.Fs2HBaseConfig

import scala.concurrent.duration.FiniteDuration

object ConnectionResource {

  def make[F[_]: Async](
      config: Fs2HBaseConfig,
      extraConfig: Map[String, String] = Map.empty
  ): Resource[F, api.Connection[F]] =
    Resource
      .fromAutoCloseable(Sync[F].blocking(connectionUnsafe(config, extraConfig)))
      .map(new HBaseClientConnection(_))

  def makeTimeout[F[_]: Async](
      config: Fs2HBaseConfig,
      rpcTimeout: FiniteDuration,
      extraConfig: Map[String, String] = Map.empty
  ): Resource[F, api.Connection[F]] =
    make(config, extraConfig).map(new TimeoutConnection(_, rpcTimeout)).timeoutAndForget(rpcTimeout)

  private def connectionUnsafe(
      config: Fs2HBaseConfig,
      extraConfig: Map[String, String]
  ): hbase.client.Connection = {
    val hbaseConfig = new conf.Configuration()
    hbaseConfig.set(HConstants.ZOOKEEPER_QUORUM, config.zooKeeperEnsemble.toList.mkString(","))
    hbaseConfig.set("hbase.zookeeper.property.clientPort", config.port.toString)
    hbaseConfig.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, config.retries.toString)
    hbaseConfig.set(HConstants.HBASE_RPC_TIMEOUT_KEY, config.rpcTimeout.toString)
    hbaseConfig.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, config.scanTimeout.toString)
    extraConfig.foreach { case (key, value) => hbaseConfig.set(key, value) }
    hbase.client.ConnectionFactory.createConnection(hbaseConfig)
  }
}
