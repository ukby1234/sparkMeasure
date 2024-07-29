package ch.cern.sparkmeasure

import org.apache.spark.SparkConf
import org.slf4j.{Logger, LoggerFactory}

class PushGatewayReporter(conf: SparkConf) extends PrometheusReporter {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  // Initialize PushGateway connection
  private val gateway: PushGateway = PushGateway(
    Utils.parsePushGatewayConfig(conf, logger)
  )

  override def report[T](metricsType: String, metrics: Map[String, T], appId: String): Unit = {
    var strMetrics = s""
    metrics.foreach {
      case (metric: String, value: Long) =>
        strMetrics += gateway.validateMetric(metric.toLowerCase()) + s" " + value.toString + s"\n"
      case (_, _) => // Discard
    }

    gateway.post(strMetrics, metricsType, "appid", appId)
  }
}
