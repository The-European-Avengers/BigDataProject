package ua.playground.benchmark.models

/**
 * Models for Latency Monitor
 */

/** Raw latency data point from aggregated output */
case class LatencyDataPoint(
                             windowStart: String,
                             windowEnd: String,
                             metric: String,
                             stationId: Int,
                             stationName: String,
                             messageCount: Long,
                             minProducerTs: Long,      // When first message was produced
                             processingEndTs: Long,    // When aggregation completed
                             latencyMs: Double         // Calculated: processingEndTs - minProducerTs
                           )

/** Aggregated latency statistics */
case class LatencyMetrics(
                           avgLatencyMs: Double,
                           p50LatencyMs: Double,     // Median
                           p95LatencyMs: Double,
                           p99LatencyMs: Double,
                           minLatencyMs: Double,
                           maxLatencyMs: Double,
                           stdDevLatencyMs: Double,
                           sampleCount: Int,
                           testThroughput: Int,
                           timestamp: String
                         )

/** Complete test report */
case class LatencyReport(
                          testName: String,
                          targetThroughput: Int,
                          metrics: LatencyMetrics,
                          dataPoints: Seq[LatencyDataPoint],
                          summary: String
                        )

/** Configuration for Latency Monitor */
case class MonitorConfig(
                          kafkaBootstrapServers: String,
                          schemaRegistryUrl: String,
                          inputTopic: String,
                          groupId: String,
                          maxWaitTimeMs: Int,
                          maxEmptyPolls: Int,
                          exportToPrometheus: Boolean,
                          prometheusPort: Int
                        )