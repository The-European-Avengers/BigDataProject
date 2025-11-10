package ua.playground.benchmark

import ua.playground.benchmark.models.*
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.Schema
import io.confluent.kafka.serializers.KafkaAvroSerializer // FIX: Corrected import path

import scala.io.Source
import java.util.Properties

object KafkaWeatherProducer:

  def main(args: Array[String]): Unit =
    // Параметры: throughput (msg/s) и тип датасета (wind/sunshine)
    val throughput = if args.length > 0 then args(0).toInt else 100
    val datasetType = if args.length > 1 then args(1) else "wind"

    println(s"=== Starting Weather Producer ===")
    println(s"Dataset: $datasetType")
    println(s"Throughput: $throughput msg/s")
    println(s"=" * 40)

    val producer = createProducer()
    val schema = createAvroSchema()

    try
      datasetType.toLowerCase match
        case "wind" => produceWindData(producer, schema, throughput)
        case "sunshine" => produceSunshineData(producer, schema, throughput)
        case _ =>
          println(s"❌ Unknown dataset type: $datasetType")
          println("Available types: wind, sunshine")
    catch
      case e: Exception =>
        println(s"❌ Error: ${e.getMessage}")
        e.printStackTrace()
    finally
      println("\nClosing producer...")
      producer.close()
      println("Producer closed.")

  def createProducer(): KafkaProducer[Integer, GenericRecord] =
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.IntegerSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    props.put("schema.registry.url", "http://localhost:8081")
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "10")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")

    new KafkaProducer[Integer, GenericRecord](props)

  def createAvroSchema(): Schema =
    // --- EDITED TO LOAD SCHEMA FROM resources/weather.avsc ---
    val resourceStream = Option(getClass.getClassLoader.getResourceAsStream("weather.avsc"))

    val schemaString = resourceStream match {
      case Some(is) => Source.fromInputStream(is).mkString
      case None =>
        // Fallback to inline string if file not found (useful for development),
        // but this is the critical schema that must match the consumer!
        println("⚠️ WARNING: weather.avsc not found in resources. Using fallback inline schema.")
        """
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
    }
    new Schema.Parser().parse(schemaString)
  // --- END EDITED SECTION ---

  def produceWindData(
                       producer: KafkaProducer[Integer, GenericRecord],
                       schema: Schema,
                       throughput: Int
                     ): Unit =
    val topic = "weather.wind"
    val csvFile = "data/wind_test.csv"

    println(s"\n📖 Reading data from: $csvFile")

    val lines = try {
      Source.fromFile(csvFile).getLines().drop(1).toList // skip header
    } catch {
      case e: Exception =>
        println(s"❌ Cannot read file: ${e.getMessage}")
        return
    }

    val delayMs = if throughput > 0 then 1000 / throughput else 0

    println(s"📊 Total messages to send: ${lines.size}")
    println(s"⏱️  Delay between messages: ${delayMs}ms")
    println(s"🎯 Target throughput: $throughput msg/s")
    println(s"📡 Kafka topic: $topic")
    println(s"\n${"=" * 40}")
    println("Starting to send messages...\n")

    var messageCount = 0
    var successCount = 0
    var errorCount = 0
    val startTime = System.currentTimeMillis()

    for line <- lines do
      val parts = line.split(",").map(_.trim)

      if parts.length >= 4 then
        try
          // CRITICAL: Get producer timestamp (start time of latency)
          val producerTimestamp = System.currentTimeMillis()

          // Создаем Avro record
          val record = new GenericData.Record(schema)
          record.put("timeObserved", parts(0))
          record.put("stationId", parts(1).toInt)
          record.put("stationName", parts(2))
          record.put("metric", "wind_speed")
          record.put("value", parts(3).toDouble)
          record.put("producer_ts", producerTimestamp) // CRITICAL: Add timestamp

          // Key для партиционирования по stationId
          val key = parts(1).toInt
          //val key = s"${parts(1)}-${lines.indexOf(line)}"
          
          val producerRecord = new ProducerRecord[Integer, GenericRecord](topic, key, record)

          // Отправляем асинхронно с callback
          producer.send(producerRecord, (metadata, exception) => {
            if exception != null then
              errorCount += 1
              println(s"❌ Error sending message: ${exception.getMessage}")
            else
              successCount += 1
              messageCount += 1

              if messageCount % 10 == 0 then
                val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                val actualThroughput = if elapsed > 0 then messageCount / elapsed else 0
                print(s"\r📤 Sent: $messageCount msgs | ✅ Success: $successCount | " +
                  s"❌ Errors: $errorCount | 📊 Rate: ${actualThroughput.formatted("%.1f")} msg/s")
          })

          // Контроль throughput
          if delayMs > 0 then
            Thread.sleep(delayMs)

        catch
          case e: Exception =>
            errorCount += 1
            println(s"\n❌ Error processing line: ${e.getMessage}")

    // Ждем отправки всех сообщений
    producer.flush()

    val endTime = System.currentTimeMillis()
    val durationSec = (endTime - startTime) / 1000.0
    val actualThroughput = if durationSec > 0 then successCount / durationSec else 0

    println(s"\n\n${"=" * 40}")
    println("✅ PRODUCTION COMPLETED")
    println(s"=" * 40)
    println(f"📊 Messages sent: $successCount")
    println(f"❌ Errors: $errorCount")
    println(f"⏱️  Duration: ${durationSec}%.2f seconds")
    println(f"📈 Actual throughput: ${actualThroughput}%.2f msg/s")
    println(f"🎯 Target throughput: $throughput msg/s")
    println(f"📉 Efficiency: ${(actualThroughput / throughput * 100)}%.1f%%")
    println(s"=" * 40)

  def produceSunshineData(
                           producer: KafkaProducer[Integer, GenericRecord],
                           schema: Schema,
                           throughput: Int
                         ): Unit =
    val topic = "weather.sunshine"
    val csvFile = "data/sunshine_test.csv"

    println(s"\n📖 Reading data from: $csvFile")

    val lines = try {
      Source.fromFile(csvFile).getLines().drop(1).toList
    } catch {
      case e: Exception =>
        println(s"❌ Cannot read file: ${e.getMessage}")
        return
    }

    val delayMs = if throughput > 0 then 1000 / throughput else 0

    println(s"📊 Total messages to send: ${lines.size}")
    println(s"⏱️  Delay between messages: ${delayMs}ms")
    println(s"🎯 Target throughput: $throughput msg/s")
    println(s"📡 Kafka topic: $topic")
    println(s"\n${"=" * 40}")
    println("Starting to send messages...\n")

    var messageCount = 0
    var successCount = 0
    var errorCount = 0
    val startTime = System.currentTimeMillis()

    for line <- lines do
      val parts = line.split(",").map(_.trim)

      if parts.length >= 4 then
        try
          // CRITICAL: Get producer timestamp (start time of latency)
          val producerTimestamp = System.currentTimeMillis()

          val record = new GenericData.Record(schema)
          record.put("timeObserved", parts(0))
          record.put("stationId", parts(1).toInt)
          record.put("stationName", parts(2))
          record.put("metric", "sunshine")
          record.put("value", parts(3).toDouble)
          record.put("producer_ts", producerTimestamp) // CRITICAL: Add timestamp

          val key = parts(1).toInt
          //val key = s"${parts(1)}-${lines.indexOf(line)}"
          val producerRecord = new ProducerRecord[Integer, GenericRecord](topic, key, record)

          producer.send(producerRecord, (metadata, exception) => {
            if exception != null then
              errorCount += 1
              println(s"❌ Error sending message: ${exception.getMessage}")
            else
              successCount += 1
              messageCount += 1

              if messageCount % 10 == 0 then
                val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                val actualThroughput = if elapsed > 0 then messageCount / elapsed else 0
                print(s"\r📤 Sent: $messageCount msgs | ✅ Success: $successCount | " +
                  s"❌ Errors: $errorCount | 📊 Rate: ${actualThroughput.formatted("%.1f")} msg/s")
          })

          if delayMs > 0 then
            Thread.sleep(delayMs)

        catch
          case e: Exception =>
            errorCount += 1
            println(s"\n❌ Error processing line: ${e.getMessage}")

    producer.flush()

    val endTime = System.currentTimeMillis()
    val durationSec = (endTime - startTime) / 1000.0
    val actualThroughput = if durationSec > 0 then successCount / durationSec else 0

    println(s"\n\n${"=" * 40}")
    println("✅ PRODUCTION COMPLETED")
    println(s"=" * 40)
    println(f"📊 Messages sent: $successCount")
    println(f"❌ Errors: $errorCount")
    println(f"⏱️  Duration: ${durationSec}%.2f seconds")
    println(f"📈 Actual throughput: ${actualThroughput}%.2f msg/s")
    println(f"🎯 Target throughput: $throughput msg/s")
    println(f"📉 Efficiency: ${(actualThroughput / throughput * 100)}%.1f%%")
    println(s"=" * 40)
