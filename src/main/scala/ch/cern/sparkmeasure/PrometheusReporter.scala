package ch.cern.sparkmeasure

trait PrometheusReporter {
  def report[T <: Any](metricsType: String, metrics: Map[String, T], appId: String): Unit
}
