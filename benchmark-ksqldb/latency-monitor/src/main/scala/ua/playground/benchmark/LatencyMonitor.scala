package ua.playground.benchmark

import ua.playground.benchmark.models.*
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{StringDeserializer, ByteArrayDeserializer}
import org.apache.avro.{Schema, generic}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import scala.jdk.CollectionConverters.*
import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.math.{sqrt, pow}

object LatencyMonitor:

  def main(args: Array[String]): Unit =
    println("=" * 70)
    println(" 📊 LATENCY MONITOR - Streaming Benchmark Analysis")
    println("=" * 70)

    val throughput = if args.length > 0 then
      args(0).toInt
    else
      throw new IllegalArgumentException("Target throughput (msg/s) required as first argument")

    val config = loadConfig()

    println(s" Target Throughput: $throughput msg/s")
    println(s" Input Topic: ${config.inputTopic}")
    println(s" Kafka Brokers: ${config.kafkaBootstrapServers}")
    println("=" * 70)
    println()

    try
      val dataPoints = collectLatencyData(config)

      if dataPoints.isEmpty then
        println("⚠️  WARNING: No latency data found!")
        println("   Make sure:")
        println("   1. Producer has sent messages")
        println("   2. Consumer has processed and written results")
        println("   3. Output topic exists and has data")
        System.exit(1)

      val metrics = calculateMetrics(dataPoints, throughput)
      val report = createReport(throughput, metrics, dataPoints)
      printReport(report)

      if config.exportToPrometheus then
        MetricsExporter.exportToPrometheus(metrics, config.prometheusPort)

      saveReport(report)

      println("\n✅ Latency monitoring completed successfully!")
      System.exit(0)

    catch
      case e: Exception =>
        println(s"\n❌ ERROR: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)

  def loadConfig(): MonitorConfig =
    MonitorConfig(
      kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
      schemaRegistryUrl = sys.env.getOrElse("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
      inputTopic = sys.env.getOrElse("INPUT_TOPIC", "weather.aggregated.output"),
      groupId = s"latency-monitor-${System.currentTimeMillis()}",
      maxWaitTimeMs = sys.env.getOrElse("MAX_WAIT_TIME_MS", "60000").toInt,
      maxEmptyPolls = sys.env.getOrElse("MAX_EMPTY_POLLS", "10").toInt,
      exportToPrometheus = sys.env.getOrElse("EXPORT_TO_PROMETHEUS", "false").toBoolean,
      prometheusPort = sys.env.getOrElse("PROMETHEUS_PORT", "9090").toInt
    )

  def collectLatencyData(config: MonitorConfig): Seq[LatencyDataPoint] =
    val consumer = createConsumer(config)
    consumer.subscribe(List(config.inputTopic).asJava)

    // ✅ Dynamically fetch the Avro schema from Schema Registry
    import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
    import io.confluent.kafka.schemaregistry.client.rest.RestService

    println(s"🔍 Fetching schema for topic: ${config.inputTopic}")

    val subject = s"${config.inputTopic}-value"
    val schemaRegistry = new CachedSchemaRegistryClient(config.schemaRegistryUrl, 100)

    // Get latest schema metadata
    val latestSchemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject)
    val avroSchema = latestSchemaMetadata.getSchema

    println(s"✅ Schema fetched successfully (ID: ${latestSchemaMetadata.getId})")

    val schema = new org.apache.avro.Schema.Parser().parse(avroSchema)
    val datumReader = new org.apache.avro.generic.GenericDatumReader[org.apache.avro.generic.GenericRecord](schema)

    val dataPoints = scala.collection.mutable.ListBuffer[LatencyDataPoint]()
    val startTime = System.currentTimeMillis()

    println(s"📊 Reading latency data from topic: ${config.inputTopic}")
    println(s"⏱️  Max wait time: ${config.maxWaitTimeMs / 1000} seconds")
    println(s"⏱️  Max empty polls: ${config.maxEmptyPolls}")
    println()

    var recordCount = 0
    var emptyPollCount = 0
    var firstRecord = true
    var detectedFormat = ""  // Track what format we detected

    try
      while (System.currentTimeMillis() - startTime) < config.maxWaitTimeMs
        && emptyPollCount < config.maxEmptyPolls do

        val records = consumer.poll(Duration.ofMillis(1000))

        if records.isEmpty then
          emptyPollCount += 1
          print(".")
        else
          emptyPollCount = 0

          records.asScala.foreach { record =>
            try
              val rawBytes = record.value()

              if firstRecord then
                println(s"\n🔍 DEBUG: Received ${rawBytes.length} bytes")
                println(s"🔍 DEBUG: First 20 bytes (hex): ${rawBytes.take(20).map(b => f"$b%02x").mkString(" ")}")

              // ===================================================
              // STEP 1: Detect message format
              // ===================================================
              val bytesToDecode = if (rawBytes.length > 5 &&
                rawBytes(0) == 0 && rawBytes(1) == 0 &&
                rawBytes(2) == 0 && rawBytes(3) == 0) {
                // Confluent Schema Registry wire format (5-byte header)
                if firstRecord then
                  detectedFormat = "Confluent (ksqlDB)"
                  val schemaIdBytes = rawBytes.slice(1, 5)
                  val schemaId = ((schemaIdBytes(0) & 0xFF) << 24) |
                    ((schemaIdBytes(1) & 0xFF) << 16) |
                    ((schemaIdBytes(2) & 0xFF) << 8) |
                    (schemaIdBytes(3) & 0xFF)
                  println(s"🔍 Detected: Confluent wire format (Schema ID: $schemaId)")

                // Skip 5-byte header
                rawBytes.drop(5)
              } else if(rawBytes.length > 5 && rawBytes(0) == 0) {
                // Confluent Schema Registry wire format (magic byte + schema ID)
                if firstRecord then
                  detectedFormat = "Confluent (ksqlDB)"
                  val schemaIdBytes = rawBytes.slice(1, 5)
                  val schemaId = java.nio.ByteBuffer.wrap(schemaIdBytes).getInt
                  println(s"🔍 Detected: Confluent wire format (Schema ID: $schemaId)")

                rawBytes.drop(5) // Skip header
              } else {
                // Direct Avro (Spark format)
                if firstRecord then
                  detectedFormat = "Direct Avro (Spark)"
                  println(s"🔍 Detected: Direct Avro format")

                rawBytes
              }

              if firstRecord then
                println(s"🔍 Decoded bytes length: ${bytesToDecode.length}")
                firstRecord = false

              // ===================================================
              // STEP 2: Deserialize Avro
              // ===================================================
              val decoder = DecoderFactory.get().binaryDecoder(bytesToDecode, null)
              val avroRecord = datumReader.read(null, decoder)

              // ===================================================
              // STEP 3: Extract fields with type safety
              // ===================================================
              def safeGet(record: GenericRecord, fieldNames: Seq[String]): Option[Any] =
                fieldNames.collectFirst {
                  case name if record.getSchema.getField(name) != null =>
                    record.get(name)
                }

              val metric = safeGet(avroRecord, Seq("metric", "METRIC")).map(_.toString).getOrElse("unknown")
              val stationId = safeGet(avroRecord, Seq("stationId", "STATIONID")).map(_.toString.toInt).getOrElse(-1)
              val stationName = safeGet(avroRecord, Seq("stationName", "STATIONNAME")).map(_.toString).getOrElse("unknown")
              val messageCount = safeGet(avroRecord, Seq("message_count", "MESSAGE_COUNT")).map(_.toString.toLong).getOrElse(0L)
              
              val windowStart = safeGet(avroRecord, Seq("window_start", "WINDOWSTART")).map(_.toString).getOrElse("N/A")
              val windowEnd = safeGet(avroRecord, Seq("window_end", "WINDOWEND")).map(_.toString).getOrElse("N/A")

              // ===================================================
              // STEP 4: Extract and validate timestamps
              // ===================================================
              val minProducerTs = safeGet(avroRecord, Seq("min_producer_ts", "MIN_PRODUCER_TS")).map(_.toString.toLong).getOrElse(0L)
              val processingEndTs = safeGet(avroRecord, Seq("processing_end_ts", "PROCESSING_END_TS")).map(_.toString.toLong).getOrElse(0L)

              if recordCount == 0 then
                println(s"✅ Deserialization successful!")
                println(s"   Format: $detectedFormat")
                println(s"   Sample values:")
                println(s"     - min_producer_ts = $minProducerTs")
                println(s"     - processing_end_ts = $processingEndTs")
                println()

              // ===================================================
              // STEP 5: Calculate and validate latency
              // ===================================================
              val latencyMs = (processingEndTs - minProducerTs).toDouble

              // Sanity checks:
              // - Latency must be positive
              // - Latency should be < 10 minutes (600000 ms)
              // - Timestamps must be reasonable (after 2020, before year 2100)
              val minProducerTsValid = minProducerTs > 1577836800000L // Jan 1, 2020
              val processingEndTsValid = processingEndTs > 1577836800000L
              val latencyValid = latencyMs > 0 && latencyMs < 600000

              if latencyValid && minProducerTsValid && processingEndTsValid then
                val dataPoint = LatencyDataPoint(
                  windowStart = windowStart,
                  windowEnd = windowEnd,
                  metric = metric,
                  stationId = stationId,
                  stationName = stationName,
                  messageCount = messageCount,
                  minProducerTs = minProducerTs,
                  processingEndTs = processingEndTs,
                  latencyMs = latencyMs
                )

                dataPoints += dataPoint
                recordCount += 1

                if recordCount % 50 == 0 then
                  print(s"\r   Records processed: $recordCount (avg latency: ${dataPoints.map(_.latencyMs).sum / dataPoints.size}%.0f ms)")
              else
                if recordCount < 5 then  // Only warn for first few invalid records
                  println(s"⚠️  Skipping invalid record:")
                  println(s"    latencyValid=$latencyValid, minProducerTsValid=$minProducerTsValid, processingEndTsValid=$processingEndTsValid")
                  println(s"    latency=${latencyMs}ms, producer=$minProducerTs, processing=$processingEndTs")

            catch
              case e: ClassCastException =>
                if recordCount < 3 then
                  println(s"\n⚠️  Type casting error (record $recordCount): ${e.getMessage}")
                  e.printStackTrace()
              case e: Exception =>
                if recordCount < 3 then
                  println(s"\n⚠️  Error processing record $recordCount: ${e.getMessage}")
                  e.printStackTrace()
          }

      println(s"\n\n✅ Collection complete!")
      println(s"   Total records processed: $recordCount")
      println(s"   Valid latency data points: ${dataPoints.size}")
      println(s"   Duration: ${(System.currentTimeMillis() - startTime) / 1000.0}%.2f seconds")
      println(s"   Detected format: $detectedFormat")
      println()

      dataPoints.toSeq

    finally
      consumer.close()

  def calculateMetrics(dataPoints: Seq[LatencyDataPoint], throughput: Int): LatencyMetrics =
    require(dataPoints.nonEmpty, "No data points to calculate metrics")

    val latencies = dataPoints.map(_.latencyMs).sorted
    val count = latencies.size
    val sum = latencies.sum
    val avg = sum / count
    val min = latencies.head
    val max = latencies.last

    def percentile(p: Double): Double =
      val index = (count * p).toInt
      latencies(Math.min(index, count - 1))

    val p50 = percentile(0.50)
    val p95 = percentile(0.95)
    val p99 = percentile(0.99)

    val variance = latencies.map(l => pow(l - avg, 2)).sum / count
    val stdDev = sqrt(variance)

    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

    LatencyMetrics(
      avgLatencyMs = avg,
      p50LatencyMs = p50,
      p95LatencyMs = p95,
      p99LatencyMs = p99,
      minLatencyMs = min,
      maxLatencyMs = max,
      stdDevLatencyMs = stdDev,
      sampleCount = count,
      testThroughput = throughput,
      timestamp = timestamp
    )

  def createReport(
                    throughput: Int,
                    metrics: LatencyMetrics,
                    dataPoints: Seq[LatencyDataPoint]
                  ): LatencyReport =
    val testName = s"Latency-Test-${throughput}msg-s"

    val summary = s"""
                     |Latency Test Summary
                     |-------------------
                     |Test: $testName
                     |Target Throughput: $throughput msg/s
                     |Sample Count: ${metrics.sampleCount}
                     |
                     |Latency Statistics:
                     |  Average: ${String.format("%.2f", metrics.avgLatencyMs)} ms
                     |  Median (P50): ${String.format("%.2f", metrics.p50LatencyMs)} ms
                     |  P95: ${String.format("%.2f", metrics.p95LatencyMs)} ms
                     |  P99: ${String.format("%.2f", metrics.p99LatencyMs)} ms
                     |  Min: ${String.format("%.2f", metrics.minLatencyMs)} ms
                     |  Max: ${String.format("%.2f", metrics.maxLatencyMs)} ms
                     |  Std Dev: ${String.format("%.2f", metrics.stdDevLatencyMs)} ms
    """.stripMargin

    LatencyReport(
      testName = testName,
      targetThroughput = throughput,
      metrics = metrics,
      dataPoints = dataPoints,
      summary = summary
    )

  def printReport(report: LatencyReport): Unit =
    println("\n" + "=" * 70)
    println(s" 📊 LATENCY TEST RESULTS: ${report.testName}")
    println("=" * 70)
    println(f" Target Throughput:    ${report.targetThroughput}%,d msg/s")
    println(f" Sample Count:         ${report.metrics.sampleCount}%,d windows")
    println(f" Timestamp:            ${report.metrics.timestamp}")
    println("-" * 70)
    println(" Latency Statistics:")
    println("-" * 70)
    println(f"   Average (Mean):     ${report.metrics.avgLatencyMs}%8.2f ms")
    println(f"   Median (P50):       ${report.metrics.p50LatencyMs}%8.2f ms")
    println(f"   P95:                ${report.metrics.p95LatencyMs}%8.2f ms")
    println(f"   P99:                ${report.metrics.p99LatencyMs}%8.2f ms")
    println(f"   Min:                ${report.metrics.minLatencyMs}%8.2f ms")
    println(f"   Max:                ${report.metrics.maxLatencyMs}%8.2f ms")
    println(f"   Std Deviation:      ${report.metrics.stdDevLatencyMs}%8.2f ms")
    println("-" * 70)
    println(" Analysis:")
    println("-" * 70)

    if report.metrics.avgLatencyMs < 5000 then
      println("   ✅ EXCELLENT - Very low latency")
    else if report.metrics.avgLatencyMs < 30000 then
      println("   ✅ GOOD - Acceptable latency")
    else if report.metrics.avgLatencyMs < 60000 then
      println("   ⚠️  MODERATE - Higher latency")
    else
      println("   ❌ HIGH - Latency needs optimization")

    if report.metrics.p99LatencyMs < 10000 then
      println("   ✅ EXCELLENT - P99 very low")
    else if report.metrics.p99LatencyMs < 60000 then
      println("   ✅ GOOD - P99 acceptable")
    else
      println("   ⚠️  P99 latency is high")

    if report.metrics.stdDevLatencyMs < 5000 then
      println("   ✅ EXCELLENT - Very consistent")
    else if report.metrics.stdDevLatencyMs < 15000 then
      println("   ✅ GOOD - Consistent")
    else
      println("   ⚠️  Some variance in latency")

    println("=" * 70)

    println("\n 🔍 Top 5 Highest Latency Windows:")
    println("-" * 70)
    report.dataPoints
      .sortBy(-_.latencyMs)
      .take(5)
      .zipWithIndex
      .foreach { case (dp, idx) =>
        println(f"   ${idx + 1}. ${dp.metric}%-12s @ ${dp.stationName}%-20s: ${dp.latencyMs}%8.2f ms")
        println(f"      Window: ${dp.windowStart} -> ${dp.windowEnd}")
      }
    println()

  def saveReport(report: LatencyReport): Unit =
    import java.io.{PrintWriter, File}

    val outputDir = sys.env.getOrElse("OUTPUT_DIR", "./benchmark-results")
    new File(outputDir).mkdirs()

    val filename = s"$outputDir/latency-report-${report.targetThroughput}msg-s-${System.currentTimeMillis()}.txt"

    val writer = new PrintWriter(new File(filename))
    try
      writer.write(report.summary)
      writer.write("\n\n")
      writer.write("=" * 70 + "\n")
      writer.write("Detailed Data Points:\n")
      writer.write("=" * 70 + "\n")
      report.dataPoints.foreach { dp =>
        writer.write(f"${dp.windowStart}%-25s ${dp.metric}%-12s ${dp.stationName}%-20s ${dp.latencyMs}%8.2f ms\n")
      }
      println(s"📄 Report saved to: $filename")
    finally
      writer.close()

  def createConsumer(config: MonitorConfig): KafkaConsumer[String, Array[Byte]] =
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500")

    new KafkaConsumer[String, Array[Byte]](props)