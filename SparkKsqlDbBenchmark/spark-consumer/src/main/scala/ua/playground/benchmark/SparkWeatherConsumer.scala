package ua.playground.benchmark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.avro.functions.{from_avro, to_avro}
import org.apache.spark.sql.types.*

import java.io.File

object SparkWeatherConsumer:

  def main(args: Array[String]): Unit =
    val throughput = if args.length > 0 then args(0).toInt else 100
    val timestamp = System.currentTimeMillis()

    // OPTIMIZATION: Configurable parameters
    val windowDuration = sys.env.getOrElse("WINDOW_DURATION", "1 minute")  // Changed from 10 minutes
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "2 seconds")  // Changed from 10 seconds
    val shufflePartitions = sys.env.getOrElse("SHUFFLE_PARTITIONS", "10").toInt  // Increased from 5

    println("=" * 60)
    println("=== Starting Spark Weather Consumer (OPTIMIZED) ===")
    println(s"Throughput: $throughput msg/s")
    println(s"Window Duration: $windowDuration")
    println(s"Trigger Interval: $triggerInterval")
    println(s"Shuffle Partitions: $shufflePartitions")
    println(s"Timestamp: $timestamp")
    println("=" * 60)

    val checkpointLocation = s"/tmp/spark/checkpoints/weather_agg_${throughput}_${timestamp}"
    cleanupCheckpoint(checkpointLocation)

    val spark = SparkSession
      .builder()
      .appName("WeatherStreamingBenchmark")
      .master("local[*]")
      // OPTIMIZATION: Tuned Spark configuration
      .config("spark.sql.shuffle.partitions", shufflePartitions.toString)
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.sql.streaming.metricsEnabled", "false")
      //Memory and performance optimizations
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.streaming.kafka.consumer.cache.enabled", "false")  // Disable cache for low latency
      .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
      .getOrCreate()

    import spark.implicits.*

    spark.sparkContext.setLogLevel("ERROR")
    org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    spark.conf.set("spark.sql.streaming.schemaInference", "false")

    println("✅ Spark Session created (OPTIMIZED)")
    println(s"📊 Shuffle partitions: $shufflePartitions")
    println(s"🎯 Master: local[*]")
    println(s"💾 Checkpoint location: $checkpointLocation")
    println(s"\n${"=" * 60}")

    val aggregatedAvroSchema = """
    {
      "type": "record",
      "name": "AggregatedWeather",
      "namespace": "ua.playground.benchmark",
      "fields": [
        {"name": "window_start", "type": "string"},
        {"name": "window_end", "type": "string"},
        {"name": "metric", "type": "string"},
        {"name": "stationId", "type": "int"},
        {"name": "stationName", "type": "string"},
        {"name": "avg_value", "type": "double"},
        {"name": "min_value", "type": "double"},
        {"name": "max_value", "type": "double"},
        {"name": "message_count", "type": "long"},
        {"name": "min_producer_ts", "type": "long"},
        {"name": "processing_end_ts", "type": "long"}
      ]
    }
    """

    try
      println("\n📡 Connecting to Kafka topics...")
      val windStream = readKafkaStream(spark, "weather.wind", "wind")
      val sunshineStream = readKafkaStream(spark, "weather.sunshine", "sunshine")
      println("✅ Connected to Kafka topics: weather.wind, weather.sunshine")

      println("\n🔄 Creating windowed aggregation...")
      val aggregatedStream = createWindowedAggregation(
        spark,
        Seq(windStream, sunshineStream),
        windowDuration
      )
      println(s"✅ Windowed aggregation created ($windowDuration windows)")

      println(s"\n${"=" * 60}")
      println("🚀 STARTING STREAMING QUERY TO KAFKA (OPTIMIZED)")
      println(s"${"=" * 60}\n")

      val finalKafkaTopic = "weather.aggregated.output"

      val kafkaOutputDF = aggregatedStream
        .withColumn("key", $"stationId".cast(StringType))
        .withColumn("value", to_avro(
          struct(
            $"window_start".cast(StringType).as("window_start"),
            $"window_end".cast(StringType).as("window_end"),
            $"metric",
            $"stationId",
            $"stationName",
            $"avg_value",
            $"min_value",
            $"max_value",
            $"message_count",
            $"min_producer_ts",
            $"processing_end_ts"
          ),
          aggregatedAvroSchema
        ))
        .select("key", "value")

      val kafkaSinkQuery = kafkaOutputDF.writeStream
        .format("kafka")
        .outputMode("update")
        .option("kafka.bootstrap.servers", sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))        .option("topic", finalKafkaTopic)
        .option("checkpointLocation", checkpointLocation)
        // OPTIMIZATION: Add Kafka producer configs for performance
        .option("kafka.compression.type", "snappy")
        .option("kafka.batch.size", "16384")
        .option("kafka.linger.ms", "0")  // Send immediately for low latency
        .option("kafka.acks", "1")  // Don't wait for all replicas
        .queryName("KafkaAggregatedSink")
        .trigger(Trigger.ProcessingTime(triggerInterval))
        .start()

      println(s"✅ Aggregated results streaming to: $finalKafkaTopic")
      println(s"✅ Output mode: update (only changed windows)")
      println(s"✅ Trigger interval: $triggerInterval")
      println(s"✅ Window duration: $windowDuration")
      println(s"✅ Checkpoint: $checkpointLocation")

      monitorProgress(kafkaSinkQuery)
      kafkaSinkQuery.awaitTermination()

    catch
      case e: Exception =>
        println(s"\n❌ Error: ${e.getMessage}")
        e.printStackTrace()
    finally
      println("\n🛑 Stopping Spark Session...")
      spark.stop()
      println("✅ Spark Session stopped")

  def readKafkaStream(
                       spark: SparkSession,
                       topic: String,
                       streamName: String
                     ): DataFrame =
    import spark.implicits.*

    println(s"  → Reading from topic: $topic")

    // OPTIMIZATION: Increased maxOffsetsPerTrigger for higher throughput
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000").toInt

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger.toString)
      .option("failOnDataLoss", "false")
      // OPTIMIZATION: Kafka consumer configs for performance
      .option("kafka.fetch.min.bytes", "1")  // Don't wait for batch to fill
      .option("kafka.fetch.max.wait.ms", "500")  // Max wait time
      .option("minPartitions", "5")  // Match Kafka partitions
      .load()

    val avroSchema = """
    {
      "type": "record",
      "name": "WeatherData",
      "namespace": "ua.playground.benchmark",
      "fields": [
        {"name": "timeObserved", "type": "string"},
        {"name": "stationId", "type": "int"},
        {"name": "stationName", "type": "string"},
        {"name": "metric", "type": "string"},
        {"name": "value", "type": "double"},
        {"name": "producer_ts", "type": "long"}
      ]
    }
    """

    // Strip Schema Registry prefix (5 bytes)
    val strippedDF = kafkaDF
      .withColumn("avro_value", expr("substring(value, 6, length(value) - 5)"))

    strippedDF
      .select(
        from_avro($"avro_value", avroSchema).as("data"),
        $"timestamp".as("kafka_timestamp"),
        $"partition",
        $"offset"
      )
      .select(
        $"data.timeObserved",
        $"data.stationId",
        $"data.stationName",
        $"data.metric",
        $"data.value",
        $"data.producer_ts",
        $"kafka_timestamp",
        $"partition",
        $"offset"
      )
      .withColumn("timestamp", to_timestamp($"timeObserved"))

  def createWindowedAggregation(
                                 spark: SparkSession,
                                 streams: Seq[DataFrame],
                                 windowDuration: String
                               ): DataFrame =
    import spark.implicits.*

    val unionStream = streams.reduce(_ union _)

    // OPTIMIZATION: Use event time instead of processing time for more accurate windowing
    // But keep processing time for low-latency benchmarks
    val useEventTime = sys.env.getOrElse("USE_EVENT_TIME", "false").toBoolean

    val timeColumn = if useEventTime then "timestamp" else "processing_time"

    val streamWithTime = if useEventTime then
      unionStream
    else
      unionStream.withColumn("processing_time", current_timestamp())

    streamWithTime
      .groupBy(
        window(col(timeColumn), windowDuration),
        $"metric",
        $"stationId",
        $"stationName"
      )
      .agg(
        avg("value").as("avg_value"),
        min("value").as("min_value"),
        max("value").as("max_value"),
        count("*").as("message_count"),
        min("producer_ts").as("min_producer_ts")
      )
      .select(
        $"window.start".cast(StringType).as("window_start"),
        $"window.end".cast(StringType).as("window_end"),
        $"metric",
        $"stationId",
        $"stationName",
        round($"avg_value", 2).as("avg_value"),
        round($"min_value", 2).as("min_value"),
        round($"max_value", 2).as("max_value"),
        $"message_count",
        $"min_producer_ts",
        (unix_timestamp(current_timestamp()) * 1000).as("processing_end_ts")
      )

  def cleanupCheckpoint(path: String): Unit =
    try
      val dir = new File(path)
      if dir.exists() then
        def deleteRecursively(file: File): Unit =
          if file.isDirectory then
            file.listFiles().foreach(deleteRecursively)
          file.delete()

        deleteRecursively(dir)
        println(s"🧹 Cleaned old checkpoint: $path")
      else
        println(s"✅ No existing checkpoint at: $path")
    catch
      case e: Exception =>
        println(s"⚠️  Warning: Could not clean checkpoint: ${e.getMessage}")

  def monitorProgress(query: StreamingQuery): Unit =
    new Thread(() => {
      Thread.sleep(10000)

      while query.isActive do
        Thread.sleep(30000)

        println(s"\n${"=" * 60}")
        println("📊 STREAMING PROGRESS REPORT")
        println(s"${"=" * 60}")

        if query.isActive then
          val progress = query.lastProgress

          if progress != null then
            println(s"\n🔹 Query: ${query.name}")
            println(s"   Status: ${query.status.message}")
            println(f"   Input rows: ${progress.numInputRows}")
            println(f"   Processing rate: ${progress.processedRowsPerSecond}%.2f rows/s")
            println(f"   Batch duration: ${progress.batchDuration} ms")

            if progress.sources.nonEmpty then
              println("   Sources:")
              progress.sources.foreach { source =>
                println(f"     • ${source.description}: ${source.numInputRows} rows")
              }

        println(s"\n${"=" * 60}\n")

    }).start()