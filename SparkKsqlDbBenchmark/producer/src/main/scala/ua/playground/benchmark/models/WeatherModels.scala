package ua.playground.benchmark

package object models:

  // ===== INPUT MODELS (из CSV) =====

  case class WindData(
                       timeObserved: String,
                       stationId: Int,
                       stationName: String,
                       mean_wind_speed: Double
                     )

  case class SunshineData(
                           timeObserved: String,
                           stationId: Int,
                           stationName: String,
                           bright_sunshine: Double
                         )

  // ===== KAFKA MODELS  =====

  case class WeatherData(
                          timeObserved: String,
                          stationId: Int,
                          stationName: String,
                          metric: String,        // "wind_speed" или "sunshine"
                          value: Double,
                          producer_ts: Long  
                        )

  // ===== OUTPUT MODELS =====
  
  case class WeatherAggregation(
                                 windowStart: String,
                                 windowEnd: String,
                                 metric: String,
                                 stationId: Int,
                                 stationName: String,
                                 avgValue: Double,
                                 minValue: Double,
                                 maxValue: Double,
                                 messageCount: Long,
                                 minProducerTs: Long,      
                                 processingEndTs: Long     
                               )

  // ===== BENCHMARK MODELS =====
  
  case class BenchmarkMetrics(
                               throughputMsgPerSec: Int,
                               avgLatencyMs: Double,
                               p95LatencyMs: Double,
                               p99LatencyMs: Double,
                               avgCpuUsagePercent: Double,
                               avgMemoryUsageMB: Double,
                               messagesProcessed: Long,
                               testDurationSeconds: Long
                             )
  
  case class TestResult(
                         testName: String,
                         targetThroughput: Int,
                         metrics: BenchmarkMetrics,
                         timestamp: String
                       )