package ua.playground.benchmark

import ua.playground.benchmark.models.LatencyMetrics
import java.io.{PrintWriter, StringWriter}
import java.net.{InetSocketAddress, ServerSocket}
import com.sun.net.httpserver.{HttpServer, HttpHandler, HttpExchange}

/**
 * Export metrics to external systems
 */
object MetricsExporter:

  /**
   * Export metrics to Prometheus format
   */
  def exportToPrometheus(metrics: LatencyMetrics, port: Int): Unit =
    println(s"\n📊 Starting Prometheus metrics exporter on port $port...")

    val server = HttpServer.create(new InetSocketAddress(port), 0)

    server.createContext("/metrics", new HttpHandler {
      def handle(exchange: HttpExchange): Unit =
        val response = generatePrometheusMetrics(metrics)
        exchange.sendResponseHeaders(200, response.length())
        val os = exchange.getResponseBody
        os.write(response.getBytes)
        os.close()
    })

    server.setExecutor(null)
    server.start()

    println(s"✅ Prometheus metrics available at: http://localhost:$port/metrics")
    println("   Press Ctrl+C to stop...")

    // Keep server running (in K8s, pod will be killed after job completes)
    Thread.sleep(Long.MaxValue)

  /**
   * Generate Prometheus format metrics
   */
  def generatePrometheusMetrics(metrics: LatencyMetrics): String =
    s"""
       |# HELP streaming_latency_avg_ms Average end-to-end latency in milliseconds
       |# TYPE streaming_latency_avg_ms gauge
       |streaming_latency_avg_ms{throughput="${metrics.testThroughput}"} ${metrics.avgLatencyMs}
       |
       |# HELP streaming_latency_p50_ms Median (P50) latency in milliseconds
       |# TYPE streaming_latency_p50_ms gauge
       |streaming_latency_p50_ms{throughput="${metrics.testThroughput}"} ${metrics.p50LatencyMs}
       |
       |# HELP streaming_latency_p95_ms P95 latency in milliseconds
       |# TYPE streaming_latency_p95_ms gauge
       |streaming_latency_p95_ms{throughput="${metrics.testThroughput}"} ${metrics.p95LatencyMs}
       |
       |# HELP streaming_latency_p99_ms P99 latency in milliseconds
       |# TYPE streaming_latency_p99_ms gauge
       |streaming_latency_p99_ms{throughput="${metrics.testThroughput}"} ${metrics.p99LatencyMs}
       |
       |# HELP streaming_latency_min_ms Minimum latency in milliseconds
       |# TYPE streaming_latency_min_ms gauge
       |streaming_latency_min_ms{throughput="${metrics.testThroughput}"} ${metrics.minLatencyMs}
       |
       |# HELP streaming_latency_max_ms Maximum latency in milliseconds
       |# TYPE streaming_latency_max_ms gauge
       |streaming_latency_max_ms{throughput="${metrics.testThroughput}"} ${metrics.maxLatencyMs}
       |
       |# HELP streaming_latency_stddev_ms Standard deviation of latency
       |# TYPE streaming_latency_stddev_ms gauge
       |streaming_latency_stddev_ms{throughput="${metrics.testThroughput}"} ${metrics.stdDevLatencyMs}
       |
       |# HELP streaming_sample_count Number of latency samples collected
       |# TYPE streaming_sample_count gauge
       |streaming_sample_count{throughput="${metrics.testThroughput}"} ${metrics.sampleCount}
       |""".stripMargin

  /**
   * Export metrics to JSON file
   */
  def exportToJson(metrics: LatencyMetrics, filename: String): Unit =
    import java.io.{File, PrintWriter}

    val json = s"""
                  |{
                  |  "test_throughput": ${metrics.testThroughput},
                  |  "avg_latency_ms": ${metrics.avgLatencyMs},
                  |  "p50_latency_ms": ${metrics.p50LatencyMs},
                  |  "p95_latency_ms": ${metrics.p95LatencyMs},
                  |  "p99_latency_ms": ${metrics.p99LatencyMs},
                  |  "min_latency_ms": ${metrics.minLatencyMs},
                  |  "max_latency_ms": ${metrics.maxLatencyMs},
                  |  "stddev_latency_ms": ${metrics.stdDevLatencyMs},
                  |  "sample_count": ${metrics.sampleCount},
                  |  "timestamp": "${metrics.timestamp}"
                  |}
                  |""".stripMargin

    val writer = new PrintWriter(new File(filename))
    try
      writer.write(json)
      println(s"📄 Metrics exported to JSON: $filename")
    finally
      writer.close()

  /**
   * Send metrics to external webhook (e.g., Slack)
   */
  def sendToWebhook(metrics: LatencyMetrics, webhookUrl: String): Unit =
    // Implementation for sending to Slack/Discord/etc
    // Left as exercise - use HTTP client to POST metrics
    println(s"📤 Sending metrics to webhook: $webhookUrl")