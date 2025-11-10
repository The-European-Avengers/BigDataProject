package ua.playground.benchmark

import ua.playground.benchmark.models.*
import java.lang.management.ManagementFactory
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object BenchmarkRunner:

  def main(args: Array[String]): Unit =
    val throughput = if args.length > 0 then
      args(0).toInt
    else
      throw new IllegalArgumentException("Target throughput (msg/s) required")

    println("=" * 70)
    println(" 🚀 KAFKA PRODUCER BENCHMARK")
    println("=" * 70)
    println(s" Target Throughput: $throughput msg/s per topic")
    println("=" * 70)

    val result = runBenchmark(throughput)
    printFinalReport(Seq(result))

  def runBenchmark(throughput: Int): TestResult =
    val testName = s"Producer-${throughput}msg-s"
    val startTime = System.currentTimeMillis()

    println(s"📋 Test configuration:")
    println(s"   • Throughput: $throughput msg/s per topic")
    println(s"   • Total throughput: ${throughput * 2} msg/s (wind + sunshine)")
    println()

    // Start producers
    println("🚀 Starting producers...")

    val windProducerThread = new Thread(() => {
      try
        KafkaWeatherProducer.main(Array(throughput.toString, "wind"))
      catch
        case e: Exception =>
          println(s"❌ Wind producer error: ${e.getMessage}")
    })

    val sunshineProducerThread = new Thread(() => {
      try
        KafkaWeatherProducer.main(Array(throughput.toString, "sunshine"))
      catch
        case e: Exception =>
          println(s"❌ Sunshine producer error: ${e.getMessage}")
    })

    windProducerThread.start()
    Thread.sleep(2000)
    sunshineProducerThread.start()

    println("✅ Producers started\n")

    Thread.sleep(5000)

    println("📊 Starting resource monitoring...\n")

    // Monitor resources
    val metrics = monitorResources(60, throughput)

    println("\n⏳ Waiting for producers to complete...")
    windProducerThread.join(60000)
    sunshineProducerThread.join(60000)

    val endTime = System.currentTimeMillis()
    val durationSeconds = (endTime - startTime) / 1000

    val result = TestResult(
      testName = testName,
      targetThroughput = throughput,
      metrics = metrics.copy(testDurationSeconds = durationSeconds),
      timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    )

    printTestResult(result)

    // ✅ Exit cleanly (K8s Job completes)
    println("\n✅ Producer job completed. Exiting.")
    result

  def monitorResources(durationSeconds: Int, throughput: Int): BenchmarkMetrics =
    // Same as before, but WITHOUT latency calculation
    // Latency will be calculated by separate LatencyMonitor
    val runtime = Runtime.getRuntime
    val osBean = ManagementFactory.getOperatingSystemMXBean

    var totalCpu = 0.0
    var totalMemory = 0.0
    var samples = 0

    val startTime = System.currentTimeMillis()
    var messagesProcessed = 0L

    println("⏱️  Monitoring (60 seconds):")
    print("   Progress: ")

    while (System.currentTimeMillis() - startTime) < durationSeconds * 1000 do
      try
        val cpuLoad = osBean.getSystemLoadAverage
        val normalizedCpu = if cpuLoad >= 0 then cpuLoad * 100 / runtime.availableProcessors() else 0

        val memoryUsed = (runtime.totalMemory() - runtime.freeMemory()) / (1024.0 * 1024.0)

        totalCpu += normalizedCpu
        totalMemory += memoryUsed
        samples += 1

        messagesProcessed += throughput * 2

        val progress = ((System.currentTimeMillis() - startTime) / 1000.0 / durationSeconds * 100).toInt
        if progress % 5 == 0 && samples % 5 == 1 then
          print(s"$progress% ")

        Thread.sleep(1000)
      catch
        case e: Exception =>
          println(s"\n⚠️  Monitoring warning: ${e.getMessage}")

    println("100% ✅\n")

    val avgCpu = if samples > 0 then totalCpu / samples else 0
    val avgMemory = if samples > 0 then totalMemory / samples else 0

    BenchmarkMetrics(
      throughputMsgPerSec = throughput * 2,
      avgLatencyMs = 0.0,  // Will be calculated by LatencyMonitor
      p95LatencyMs = 0.0,
      p99LatencyMs = 0.0,
      avgCpuUsagePercent = avgCpu,
      avgMemoryUsageMB = avgMemory,
      messagesProcessed = messagesProcessed,
      testDurationSeconds = durationSeconds
    )

  def printTestResult(result: TestResult): Unit =
    println(s"\n${"=" * 70}")
    println(s" 📊 PRODUCER RESULTS: ${result.testName}")
    println(s"${"=" * 70}")
    println(f" Target Throughput:    ${result.targetThroughput}%,d msg/s per topic")
    println(f" Total Throughput:     ${result.metrics.throughputMsgPerSec}%,d msg/s")
    println(f" Messages Processed:   ${result.metrics.messagesProcessed}%,d")
    println(f" Test Duration:        ${result.metrics.testDurationSeconds} seconds")
    println(s"${"-" * 70}")
    println(f" Avg CPU Usage:        ${result.metrics.avgCpuUsagePercent}%.2f%%")
    println(f" Avg Memory Usage:     ${result.metrics.avgMemoryUsageMB}%.2f MB")
    println(s"${"=" * 70}\n")

  def printFinalReport(results: Seq[TestResult]): Unit =
    println("\n" + "=" * 70)
    println(" 📈 PRODUCER BENCHMARK COMPLETE")
    println("=" * 70)
    println(" ✅ Job will now terminate")
    println("=" * 70)