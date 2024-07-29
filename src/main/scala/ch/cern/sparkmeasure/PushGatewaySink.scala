package ch.cern.sparkmeasure

import org.apache.spark.SparkConf

/**
 * PushGatewaySink: write Spark metrics and application info in near real-time to Prometheus Push Gateway
 * use this mode to monitor Spark execution workload
 * use for Grafana dashboard and analytics of job execution
 * Limitation: only metrics with numeric values are reported to the Push Gateway
 *
 * How to use: attach the PushGatewaySink to a Spark Context using the extra listener infrastructure.
 * Example:
 * --conf spark.extraListeners=ch.cern.sparkmeasure.PushGatewaySink
 *
 * Configuration for PushGatewaySink is handled with Spark conf parameters:
 * spark.sparkmeasure.pushgateway = SERVER:PORT // Prometheus Push Gateway URL
 * spark.sparkmeasure.pushgateway.jobname // value for the job label, default pushgateway
 * Example: --conf spark.sparkmeasure.pushgateway=localhost:9091
 *
 * Output: each message contains the metric name and value, only numeric values are used
 * Note: the amount of data generated is relatively small in most applications: O(number_of_stages)
 */
class PushGatewaySink(conf: SparkConf) extends GenericPrometheusSink(conf = conf) {
  override def prometheusReporter: PrometheusReporter = new PushGatewayReporter(conf)
}
