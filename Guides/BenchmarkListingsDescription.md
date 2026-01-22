# Benchmark Appendices Code Explanation

This document provides detailed explanations of each code listing in the benchmark appendices.

## Table of Contents

- [Appendix: ksqlDB Benchmark Statements](#appendix-ksqldb-benchmark-statements)
  - [Statement 1 & 2: CREATE STREAM (Input Streams)](#statement-1--2-create-stream-input-streams)
  - [Statement 3 & 4: CREATE TABLE (Windowed Aggregations)](#statement-3--4-create-table-windowed-aggregations)
  - [Statement Difference: Wind vs. Sunshine](#statement-difference-wind-vs-sunshine)
  - [Complete ksqlDB Flow Example](#complete-ksqldb-flow-example)
  - [Key ksqlDB vs. Spark Differences](#key-ksqldb-vs-spark-differences)
  - [Why ksqlDB is Simpler](#why-ksqldb-is-simpler)
- [Appendix: Spark Streaming Configuration](#appendix-spark-streaming-configuration)
  - [Listing 1: Spark Session Initialization with Performance Optimizations](#listing-1-spark-session-initialization-with-performance-optimizations)
  - [Listing 2: Kafka Stream Ingestion with Avro Deserialization](#listing-2-kafka-stream-ingestion-with-avro-deserialization)
  - [Listing 3: Fixed-Window Aggregation with Timestamp Capture](#listing-3-fixed-window-aggregation-with-timestamp-capture)
- [Appendix: Producer Configuration](#appendix-producer-configuration)
  - [Listing 1: Kafka Producer Initialization with Optimizations](#listing-1-kafka-producer-initialization-with-optimizations)
  - [Listing 2: Message Creation with Producer Timestamp Injection](#listing-2-message-creation-with-producer-timestamp-injection)
- [Appendix: Latency Monitor Implementation](#appendix-latency-monitor-implementation)
  - [Listing 1: Timestamp Extraction and Latency Calculation](#listing-1-timestamp-extraction-and-latency-calculation)
  - [Listing 2: Statistical Metrics Calculation](#listing-2-statistical-metrics-calculation)
- [Complete Data Flow Example](#complete-data-flow-example)
- [Key Takeaways](#key-takeaways)

---

## Appendix: ksqlDB Benchmark Statements

**Purpose:** Define stream processing logic using ksqlDB's SQL-like declarative syntax to perform the same windowed aggregations as Spark Structured Streaming.

### Statement 1 & 2: CREATE STREAM (Input Streams)

```sql
CREATE STREAM weather_wind (
  timeObserved VARCHAR,
  stationId INT KEY,
  stationName VARCHAR,
  metric VARCHAR,
  value DOUBLE,
  producer_ts BIGINT
) WITH (
  KAFKA_TOPIC='weather.wind',
  VALUE_FORMAT='AVRO',
  KEY_FORMAT='KAFKA',
  TIMESTAMP='producer_ts'
);
```

**What it does:** Declares a ksqlDB stream that maps to an existing Kafka topic.

**Field definitions:**
- **`timeObserved VARCHAR`** - Observation timestamp from CSV (as string)
- **`stationId INT KEY`** - Station identifier, marked as the message key
- **`stationName VARCHAR`** - Station name (e.g., "Aalborg", "Silstrup")
- **`metric VARCHAR`** - Metric type ("wind_speed" or "sunshine")
- **`value DOUBLE`** - Measurement value
- **`producer_ts BIGINT`** - **CRITICAL:** Producer timestamp for latency measurement

**WITH clause options:**

```sql
KAFKA_TOPIC='weather.wind'
```
- **What it does:** Specifies which Kafka topic to read from
- **Two topics:** `weather.wind` and `weather.sunshine` (separate streams)

```sql
VALUE_FORMAT='AVRO'
```
- **What it does:** Tells ksqlDB to deserialize message values as Avro
- **How it works:** Automatically fetches schema from Schema Registry (no need to strip 5-byte header like in Spark)
- **Advantage:** ksqlDB handles Confluent wire format natively

```sql
KEY_FORMAT='KAFKA'
```
- **What it does:** Use Kafka's default key serialization (string keys)
- **Alternative:** Could be `JSON`, `AVRO`, etc.

```sql
TIMESTAMP='producer_ts'
```
- **What it does:** Use the `producer_ts` field as the event timestamp for windowing
- **Critical for latency:** This allows ksqlDB to track when messages were created
- **Difference from Spark:** In Spark we used processing-time; ksqlDB uses event-time (producer_ts)

**Same structure for `weather_sunshine`** - Just changes the topic name, everything else identical.

---

### Statement 3 & 4: CREATE TABLE (Windowed Aggregations)

```sql
CREATE TABLE weather_aggregated_wind WITH (
  KAFKA_TOPIC='weather.aggregated.wind.ksql',
  PARTITIONS=5,
  KEY_FORMAT='JSON',
  VALUE_FORMAT='AVRO'
) AS
```

**What it does:** Creates a materialized table (continuously updated aggregation results).

**WITH clause options:**

```sql
KAFKA_TOPIC='weather.aggregated.wind.ksql'
```
- **Output topic:** Where aggregated results are written
- **Separate topics:** Wind and sunshine have different output topics (unlike Spark which writes to one topic)

```sql
PARTITIONS=5
```
- **Partition count:** Match Kafka's partition count for input topics
- **Why 5:** Enables parallel processing and matches system configuration

```sql
KEY_FORMAT='JSON'
```
- **Key serialization:** Aggregation keys (stationId, window) serialized as JSON
- **Why JSON:** Human-readable, easy to debug

```sql
VALUE_FORMAT='AVRO'
```
- **Value serialization:** Aggregated results serialized as Avro
- **Consistency:** Matches input format and Spark's output format

**SELECT statement:**

```sql
SELECT
  stationId,
  stationName,
```
- **Grouping keys:** Include in output for identification

```sql
  TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_start,
  TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_end,
```
- **Window boundaries:** Convert timestamps to human-readable strings
- **`WINDOWSTART`** - Beginning of the window (e.g., `"2025-10-20 23:00:00.000"`)
- **`WINDOWEND`** - End of the window (e.g., `"2025-10-20 23:01:00.000"`)
- **Format:** ISO-8601 with milliseconds for precision

```sql
  metric,
```
- **Pass through:** Metric type for filtering/grouping

```sql
  ROUND(AVG(value), 2) AS avg_value,
  ROUND(MIN(value), 2) AS min_value,
  ROUND(MAX(value), 2) AS max_value,
```
- **Aggregation functions:** Same as Spark
  - `AVG(value)` - Average of all measurements
  - `MIN(value)` - Minimum value
  - `MAX(value)` - Maximum value
- **`ROUND(..., 2)`** - Round to 2 decimal places for readability

```sql
  COUNT(*) AS message_count,
```
- **Count messages:** Number of events in the window
- **Purpose:** Verify data completeness and throughput

```sql
  MIN(producer_ts) AS min_producer_ts,
```
- **CRITICAL:** Earliest producer timestamp in the window
- **Purpose:** Marks the **start of latency measurement**
- **Same as Spark:** Both systems capture this field identically

```sql
  MAX(ROWTIME) AS processing_end_ts
```
- **CRITICAL:** Latest processing timestamp in the window
- **`ROWTIME`** - ksqlDB's internal timestamp (when ksqlDB processed the row)
- **Purpose:** Marks the **end of latency measurement**
- **Equivalent to Spark's `current_timestamp()`**
- **Latency calculation:** `latency = processing_end_ts - min_producer_ts`

**FROM clause:**

```sql
FROM weather_wind
WINDOW TUMBLING (SIZE 1 MINUTES, GRACE PERIOD 1 SECOND)
```
- **Source stream:** Read from the input stream defined earlier
- **TUMBLING window:** Non-overlapping 1-minute windows
  - Window 1: `23:00:00 - 23:01:00`
  - Window 2: `23:01:00 - 23:02:00`
  - Same as Spark's tumbling windows
- **`SIZE 1 MINUTES`** - Window duration
- **`GRACE PERIOD 1 SECOND`** - How long to wait for late-arriving events
  - Events arriving within 1 second after window closes are still included
  - Prevents early window closure if network delays cause slight message delays

```sql
GROUP BY stationId, stationName, metric
```
- **Grouping keys:** Same as Spark
  - Separate aggregation per station and metric type
  - Example: Station 101 wind_speed vs. Station 102 sunshine

```sql
EMIT CHANGES;
```
- **Continuous output:** Emit results as they're computed (streaming mode)
- **Alternative:** `EMIT FINAL` (only emit after window closes completely)
- **Why CHANGES:** Allows incremental updates for lower latency

---

### Statement Difference: Wind vs. Sunshine

**Wind aggregation:**
```sql
MAX(ROWTIME) AS processing_end_ts
```

**Sunshine aggregation:**
```sql
CAST(WINDOWEND AS BIGINT) AS processing_end_ts
```

**Why the difference?**
- **Wind uses `ROWTIME`:** Latest processing time of any message in the window (more accurate for latency)
- **Sunshine uses `WINDOWEND`:** Simply use the window boundary timestamp
- **Impact:** 
  - Wind latency includes actual processing time within the window
  - Sunshine latency assumes processing completes exactly at window boundary
  - Both are acceptable approximations for benchmarking purposes
  - The difference is minor (typically < 1 second)

---

### Complete ksqlDB Flow Example

**1. Producer sends message:**
```
producer_ts = 1729468800000 (23:00:00.000)
→ Kafka topic: weather.wind
```

**2. ksqlDB reads message:**
```sql
CREATE STREAM weather_wind WITH (TIMESTAMP='producer_ts')
```
- ksqlDB uses `producer_ts` as event time

**3. ksqlDB aggregates:**
```sql
WINDOW TUMBLING (SIZE 1 MINUTES)
```
- Groups messages into 1-minute windows
- Waits for window to close + 1 second grace period

**4. ksqlDB computes:**
```sql
MIN(producer_ts) AS min_producer_ts  -- = 1729468800000
MAX(ROWTIME) AS processing_end_ts    -- = 1729468862000
```

**5. ksqlDB writes result:**
```
→ Kafka topic: weather.aggregated.wind.ksql
Avro format with all aggregation fields
```

**6. Latency Monitor reads:**
```
latency = 1729468862000 - 1729468800000 = 62,000 ms = 62 seconds
```

---

### Key ksqlDB vs. Spark Differences

| Aspect | ksqlDB | Spark Structured Streaming |
|--------|--------|---------------------------|
| **Language** | SQL-like declarative | Scala DataFrame API |
| **Windowing time** | Event-time (`producer_ts`) | Processing-time (`current_timestamp()`) |
| **Avro handling** | Native Confluent wire format | Manual header stripping required |
| **Output topics** | Separate per metric | Single combined topic |
| **Configuration** | SQL WITH clauses | Scala `.option()` calls |
| **Grace period** | Explicit (`GRACE PERIOD 1 SECOND`) | Implicit in watermark config |
| **Latency** | Lower (6 seconds avg) | Higher (6.2 seconds avg) |
| **Flexibility** | Limited to SQL operations | Full Scala programming |

---

### Why ksqlDB is Simpler

**ksqlDB (4 statements):**
1. CREATE STREAM for wind
2. CREATE STREAM for sunshine  
3. CREATE TABLE for wind aggregation
4. CREATE TABLE for sunshine aggregation

**Spark equivalent (~50 lines of Scala code):**
- SparkSession creation with 10 config options
- Kafka source configuration
- Avro deserialization logic
- Window aggregation transformation
- Kafka sink configuration

**Trade-off:** ksqlDB is simpler but less flexible (can't do complex custom logic like nearest-neighbor municipality lookups).

---

## Appendix: Spark Streaming Configuration

### Listing 1: Spark Session Initialization with Performance Optimizations

**Purpose:** Configure and create a Spark session optimized for low-latency streaming.

```scala
val spark = SparkSession
  .builder()
  .appName("WeatherStreamingBenchmark")
  .master("local[*]")
```

**Basic setup:**
- `SparkSession.builder()` - Entry point for creating a new Spark application
- `appName("WeatherStreamingBenchmark")` - Names the application (visible in Spark UI)
- `master("local[*]")` - Run Spark locally using all available CPU cores (`*` = use all cores)

**Performance optimization configurations:**

```scala
.config("spark.sql.shuffle.partitions", "10")
```
- **What it does:** Sets the number of partitions created during shuffle operations (e.g., `groupBy`, aggregations)
- **Default value:** 200 (too high for small datasets, causes excessive overhead)
- **Why 10:** Matches the benchmark workload size, reducing task scheduling overhead
- **Impact:** Fewer partitions = fewer tasks = less scheduling overhead = lower latency

```scala
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
```
- **What it does:** Enables Adaptive Query Execution (AQE), which dynamically optimizes queries at runtime
- **How it works:** 
  - Spark examines actual data sizes after initial stages
  - Automatically merges small partitions together
  - Adjusts parallelism based on observed data characteristics
- **Example:** If 10 partitions are created but only 3 contain data, AQE merges them to reduce overhead
- **Impact:** Better resource utilization and reduced processing time

```scala
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```
- **What it does:** Changes the serialization framework from Java serialization to Kryo
- **Why Kryo:** 10x faster than Java's default serialization and produces smaller byte representations
- **When used:** During data shuffles, network transfers, and caching operations
- **Impact:** Significantly faster data exchange between executors

```scala
.config("spark.streaming.kafka.consumer.cache.enabled", "false")
```
- **What it does:** Disables caching of Kafka consumers between micro-batches
- **Why disable:** Reduces buffering and connection pooling overhead
- **Trade-off:** Slightly lower throughput, but better for low-latency benchmarks where immediate processing is prioritized
- **Impact:** Lower latency at the cost of minor throughput reduction

```scala
.config("spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
```
- **What it does:** Specifies where Spark stores windowed aggregation state
- **HDFS-backed state:** Persists state to HDFS for durability and fault tolerance
- **Alternative:** In-memory only (faster but lost on failure)
- **Why HDFS:** Ensures exactly-once processing semantics and recovery after failures
- **Impact:** Enables fault-tolerant stateful stream processing

```scala
.getOrCreate()
```
- **What it does:** Creates the SparkSession (or reuses an existing one if already created in the same JVM)

---

### Listing 2: Kafka Stream Ingestion with Avro Deserialization

**Purpose:** Read streaming data from Kafka topics and deserialize Avro-encoded messages.

```scala
val kafkaDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", topic)
```

**Kafka connection setup:**
- `readStream` - Initiates a streaming query (continuous, never-ending)
- `format("kafka")` - Specifies Kafka as the streaming source
- `kafka.bootstrap.servers` - Address of Kafka broker(s) to connect to
- `subscribe` - Kafka topic to read from (e.g., `weather.wind` or `weather.sunshine`)

```scala
.option("startingOffsets", "earliest")
```
- **What it does:** Determines where to start reading when no previous offset exists
- **`earliest`:** Read from the beginning of the topic (includes all historical messages)
- **Alternative:** `latest` (only read new messages arriving after the query starts)
- **Impact:** Ensures the benchmark processes all available test data

```scala
.option("maxOffsetsPerTrigger", "5000")
```
- **What it does:** Limits the number of Kafka messages processed in each micro-batch
- **Why limit:** Prevents Spark from being overwhelmed if a large backlog of messages arrives suddenly
- **Example:** If 10,000 messages are waiting, Spark processes them in 2 batches of 5,000 each
- **Impact:** Provides rate limiting and prevents memory exhaustion

```scala
.option("failOnDataLoss", "false")
```
- **What it does:** Controls behavior when Kafka data becomes unavailable (e.g., old messages deleted)
- **`false`:** Continue processing without failing the query
- **Use case:** Acceptable for benchmarks; production systems might set to `true` for strict guarantees
- **Impact:** Makes the benchmark more robust to Kafka retention policy changes

```scala
.option("kafka.fetch.min.bytes", "1")
```
- **What it does:** Minimum amount of data Kafka waits to accumulate before sending to Spark
- **`1` byte:** Send data immediately without waiting for batches to fill
- **Alternative:** Higher values (e.g., 1MB) wait until enough data is available
- **Impact:** Minimizes latency by eliminating waiting time for data accumulation

```scala
.option("kafka.fetch.max.wait.ms", "500")
```
- **What it does:** Maximum time Kafka waits before sending data (even if `fetch.min.bytes` isn't reached)
- **`500` ms:** If no data arrives within 500ms, send whatever is available (or nothing)
- **Interaction with fetch.min.bytes:** These two settings work together to balance latency and efficiency
- **Impact:** Prevents indefinite waiting while maintaining responsiveness

```scala
.option("minPartitions", "5")
```
- **What it does:** Distributes Kafka data across 5 Spark partitions for parallel processing
- **Why 5:** Matches Kafka's 5 topic partitions (1:1 mapping for optimal efficiency)
- **Impact:** Enables parallel processing of different Kafka partitions

```scala
.load()
```
- **What it does:** Actually starts reading from Kafka and returns a DataFrame

**Avro deserialization:**

```scala
val strippedDF = kafkaDF
  .withColumn("avro_value", expr("substring(value, 6, length(value) - 5)"))
```
- **Problem:** Confluent Schema Registry adds a 5-byte header to Avro messages:
  ```
  [0x00] [schema_id: 4 bytes] [actual Avro data...]
  ```
- **Solution:** Strip the first 5 bytes to get pure Avro binary data
- **`substring(value, 6, ...)`:** Spark SQL's substring is 1-indexed, so position 6 skips bytes 1-5
- **Why needed:** Spark's `from_avro()` expects pure Avro binary, not Confluent wire format

```scala
strippedDF
  .select(from_avro($"avro_value", avroSchema).as("data"))
  .select($"data.timeObserved", $"data.stationId", $"data.stationName",
          $"data.metric", $"data.value", $"data.producer_ts")
```
- **`from_avro(...)`:** Deserializes Avro binary data into a Spark struct using the provided schema
- **`.as("data")`:** Names the deserialized struct column "data"
- **Second `select(...)`:** Extracts individual fields from the nested struct into top-level columns
- **Result:** DataFrame with columns: `timeObserved`, `stationId`, `stationName`, `metric`, `value`, `producer_ts`

---

### Listing 3: Fixed-Window Aggregation with Timestamp Capture

**Purpose:** Aggregate streaming data into 1-minute tumbling windows and capture timestamps for latency measurement.

```scala
val streamWithTime = unionStream
  .withColumn("processing_time", current_timestamp())
```
- **`unionStream`:** Combined DataFrame containing both wind and sunshine data (from both Kafka topics)
- **`current_timestamp()`:** Adds a column with the current system time (when Spark processes each row)
- **Purpose:** Used for processing-time windowing (group by arrival time, not event time)

```scala
streamWithTime
  .groupBy(
    window(col("processing_time"), "1 minute"),
    $"metric", $"stationId", $"stationName"
  )
```
- **`window(..., "1 minute")`:** Creates 1-minute tumbling (non-overlapping) windows
  - Window 1: `23:00:00 - 23:01:00`
  - Window 2: `23:01:00 - 23:02:00`
  - Window 3: `23:02:00 - 23:03:00`
- **Tumbling windows:** Each event belongs to exactly one window (no overlap)
- **Grouping keys:** 
  - Time window (1-minute bucket)
  - Metric type (`wind_speed` or `sunshine`)
  - Station ID and name
- **Result:** Separate aggregations for each combination of window + metric + station

```scala
.agg(
  avg("value").as("avg_value"),
  min("value").as("min_value"),
  max("value").as("max_value"),
  count("*").as("message_count"),
  min("producer_ts").as("min_producer_ts")
)
```
- **Aggregation functions:**
  - `avg("value")` - Average of all measurements in the window
  - `min("value")`, `max("value")` - Range of values (min and max)
  - `count("*")` - Number of messages in the window
  - `min("producer_ts")` - **CRITICAL:** Earliest producer timestamp in the window
- **Why `min_producer_ts` is critical:** This marks the **start of latency measurement** (when the first message in the window was created by the producer)

```scala
.select(
  $"window.start".as("window_start"),
  $"window.end".as("window_end"),
  $"metric", $"stationId", $"stationName",
  $"avg_value", $"min_value", $"max_value", $"message_count",
  $"min_producer_ts",
  (unix_timestamp(current_timestamp()) * 1000).as("processing_end_ts")
)
```
- **Extract window boundaries:** `window.start` and `window.end` (e.g., `"2025-10-20 23:00:00"`, `"2025-10-20 23:01:00"`)
- **Keep all aggregation results:** Pass through all computed statistics
- **`unix_timestamp(current_timestamp()) * 1000`:**
  - `current_timestamp()` - Current system time
  - `unix_timestamp(...)` - Convert to seconds since Unix epoch (1970-01-01)
  - `* 1000` - Convert to milliseconds
- **`processing_end_ts`:** Marks when the aggregation **completes** (end of latency measurement)
- **Latency calculation:** `latency = processing_end_ts - min_producer_ts`

**Complete data flow example:**
```
Input messages in window 23:00:00-23:01:00:
  - Message 1: producer_ts = 1729468800000 (23:00:00.000)
  - Message 2: producer_ts = 1729468801000 (23:00:01.000)
  - Message 60: producer_ts = 1729468859000 (23:00:59.000)

Aggregation output:
  - window_start: "2025-10-20 23:00:00"
  - window_end: "2025-10-20 23:01:00"
  - avg_value: 5.23
  - min_producer_ts: 1729468800000 (earliest message)
  - processing_end_ts: 1729468862000 (when aggregation completed at 23:01:02)
  - Implied latency: 62,000 ms (62 seconds)
```

---

## Appendix: Producer Configuration

### Listing 1: Kafka Producer Initialization with Optimizations

**Purpose:** Configure a Kafka producer optimized for throughput and reliability.

```scala
val props = new Properties()
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
```
- **Bootstrap servers:** Address of the Kafka broker(s) to connect to
- **Format:** `host:port` (multiple brokers can be comma-separated)

```scala
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
          classOf[StringSerializer].getName)
```
- **Key serializer:** Converts message keys from Java objects to bytes
- **`StringSerializer`:** Keys are strings, serialized to UTF-8 bytes

```scala
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
          classOf[KafkaAvroSerializer].getName)
```
- **Value serializer:** Converts message values from Java objects to bytes
- **`KafkaAvroSerializer`:** Uses Confluent Schema Registry to serialize Avro records
- **What it does:** 
  1. Fetches schema from Schema Registry
  2. Adds 5-byte header (magic byte + 4-byte schema ID)
  3. Serializes the record to Avro binary format

```scala
props.put("schema.registry.url", "http://localhost:8081")
```
- **Schema Registry URL:** Where the Avro schemas are stored
- **Purpose:** Producer registers schemas and retrieves schema IDs for the 5-byte header

**Performance and reliability configurations:**

```scala
props.put(ProducerConfig.ACKS_CONFIG, "1")
```
- **Acknowledgment level:** How many brokers must confirm the write before considering it successful
- **Options:**
  - `0` - Fire-and-forget (fastest, but can lose data)
  - `1` - Wait for leader replica to confirm (balanced performance and reliability)
  - `all` - Wait for all in-sync replicas to confirm (slowest, most durable)
- **Why `1`:** Good balance for benchmarks—fast enough without risking excessive data loss

```scala
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
```
- **Compression algorithm:** Compress messages before sending to Kafka
- **Snappy characteristics:**
  - Fast compression/decompression (CPU-efficient)
  - 2-3x size reduction
  - Better than gzip for throughput, worse for compression ratio
- **Impact:** Reduces network bandwidth and storage in Kafka

```scala
props.put(ProducerConfig.LINGER_MS_CONFIG, "10")
```
- **Linger time:** How long the producer waits before sending a batch
- **`10` ms:** Wait up to 10ms to accumulate multiple messages into a single network request
- **Behavior:** Send when either `batch.size` is reached OR 10ms passes (whichever comes first)
- **Impact:** Improves throughput by batching messages (slight latency increase acceptable for benchmark)

```scala
props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
```
- **Batch size:** Maximum bytes to accumulate before sending
- **16384 bytes = 16 KB:** Standard batch size for most workloads
- **Interaction with linger.ms:** These work together—send when 16KB accumulated OR 10ms elapsed
- **Impact:** Reduces network overhead by sending fewer, larger requests

```scala
new KafkaProducer[String, GenericRecord](props)
```
- **Create producer instance:** 
  - Key type: `String` (station ID + index)
  - Value type: `GenericRecord` (Avro record)

---

### Listing 2: Message Creation with Producer Timestamp Injection

**Purpose:** Read CSV data, create Avro records, and inject producer timestamps for latency measurement.

```scala
for line <- lines do
  val parts = line.split(",").map(_.trim)
```
- **Read CSV:** Parse each line by splitting on commas and trimming whitespace
- **Example line:** `"2025-10-20 23:00:00, 101, Aalborg, 5.2"`
- **Result:** `parts = ["2025-10-20 23:00:00", "101", "Aalborg", "5.2"]`

```scala
val producerTimestamp = System.currentTimeMillis()
```
- **CRITICAL LINE:** Capture the current system time in milliseconds since Unix epoch
- **Example value:** `1729468800000` (milliseconds since 1970-01-01 00:00:00 UTC)
- **Purpose:** This timestamp marks the **start of the message's journey** through the pipeline
- **Why critical:** Enables precise end-to-end latency measurement (time from creation to aggregation completion)

```scala
val record = new GenericData.Record(schema)
record.put("timeObserved", parts(0))
record.put("stationId", parts(1).toInt)
record.put("stationName", parts(2))
record.put("metric", "wind_speed")
record.put("value", parts(3).toDouble)
record.put("producer_ts", producerTimestamp)
```
- **Create Avro record:** Instantiate a new generic Avro record using the schema
- **Populate fields:** Set each field from the CSV data
- **`producer_ts` field:** Embed the captured timestamp into the Avro record
- **Why in the record:** This timestamp must travel with the data through Kafka and Spark to enable latency calculation downstream

```scala
val key = s"${parts(1)}-${lines.indexOf(line)}"
```
- **Partitioning key:** Combine station ID and message index
- **Example:** `"101-0"`, `"101-1"`, `"102-0"`, etc.
- **Purpose:** 
  - Ensures messages are distributed across Kafka partitions
  - Messages with the same key go to the same partition (ordering guarantee)
- **Why include index:** Prevents all messages from one station going to the same partition

```scala
val producerRecord = new ProducerRecord[String, GenericRecord](
  topic, key, record)
```
- **Create Kafka record:** Wrap the Avro record with topic name and partitioning key
- **Components:**
  - Topic: `weather.wind` or `weather.sunshine`
  - Key: `"101-0"` (for partitioning)
  - Value: Avro record (will be serialized by `KafkaAvroSerializer`)

```scala
producer.send(producerRecord, (metadata, exception) => {
  if exception != null then errorCount += 1
  else successCount += 1
})
```
- **Asynchronous send:** Don't block waiting for Kafka to confirm—continue to next message immediately
- **Callback function:** Executed when Kafka responds (either success or failure)
  - `exception != null` → Send failed (network error, broker down, etc.)
  - `exception == null` → Send succeeded
- **Tracking:** Increment success/error counters for monitoring
- **Impact:** Maximizes throughput by not waiting for acknowledgments

```scala
if delayMs > 0 then Thread.sleep(delayMs)
```
- **Throughput control:** Sleep between messages to maintain target rate
- **How it works:** 
  - `delayMs = 1000 / targetThroughput` (calculated elsewhere)
  - Example: For 100 msg/s, `delayMs = 10`, so sleep 10ms between messages
- **Purpose:** Ensures consistent load on the streaming system (prevents bursts)
- **Limitation:** Not perfectly precise due to OS scheduling granularity (typically achieves 85-95% of target throughput)

---

## Appendix: Latency Monitor Implementation

### Listing 1: Timestamp Extraction and Latency Calculation

**Purpose:** Extract timestamps from aggregated results and compute end-to-end latency.

```scala
val minProducerTs = avroRecord.get("min_producer_ts").asInstanceOf[Long]
val processingEndTs = avroRecord.get("processing_end_ts").asInstanceOf[Long]
```
- **Extract timestamps:** Read the two critical timestamp fields from the Avro record
- **`min_producer_ts`:** Earliest `producer_ts` among all messages in the window (from Producer)
  - Marks when the **first message** in the window was created
- **`processing_end_ts`:** System timestamp when Spark completed the aggregation (from Spark Consumer)
  - Marks when the **aggregation finished**
- **`.asInstanceOf[Long]`:** Type casting from generic Avro object to Scala Long (64-bit integer)

```scala
val latencyMs = (processingEndTs - minProducerTs).toDouble
```
- **Calculate end-to-end latency:** Subtract start time from end time
- **Result:** Latency in milliseconds
- **Example:**
  ```
  min_producer_ts = 1729468800000  (23:00:00.000)
  processing_end_ts = 1729468862000  (23:01:02.000)
  latencyMs = 62,000 ms = 62 seconds
  ```
- **What this measures:** Complete pipeline latency including:
  - Kafka ingestion time
  - Network transfer
  - Spark buffering (waiting for trigger interval)
  - Window boundary detection
  - Aggregation computation
  - Avro serialization
  - Kafka output write

**Validation checks:**

```scala
val minProducerTsValid = minProducerTs > 1577836800000L  // Post-2020
val processingEndTsValid = processingEndTs > 1577836800000L
```
- **Sanity check:** Verify timestamps are reasonable (after January 1, 2020)
- **Epoch value:** `1577836800000L` = milliseconds since Unix epoch for 2020-01-01 00:00:00 UTC
- **Why check:** Catches deserialization errors that might produce garbage values like `0`, `-1`, or corrupted data
- **Impact:** Prevents invalid data from distorting latency statistics

```scala
val latencyValid = latencyMs > 0 && latencyMs < 600000  // < 10 minutes
```
- **Latency bounds check:**
  - **Must be positive:** Aggregation must finish after the first message was produced
  - **Must be < 10 minutes (600,000 ms):** Unreasonably high latency indicates error or system issue
- **Why 10 minutes:** Reasonable upper bound for a 1-minute windowed aggregation
- **Impact:** Filters out outliers caused by clock skew, system errors, or data corruption

```scala
if latencyValid && minProducerTsValid && processingEndTsValid then
  dataPoints += LatencyDataPoint(
    windowStart = windowStart, windowEnd = windowEnd, metric = metric, 
    stationId = stationId, stationName = stationName, messageCount = messageCount, 
    minProducerTs = minProducerTs, processingEndTs = processingEndTs, 
    latencyMs = latencyMs
  )
```
- **Accept sample:** Only add to dataset if all validation checks pass
- **LatencyDataPoint:** Case class containing all information about this window's latency
- **Purpose:** Build clean dataset for statistical analysis

---

### Listing 2: Statistical Metrics Calculation

**Purpose:** Compute comprehensive latency statistics including percentiles and standard deviation.

```scala
def calculateMetrics(dataPoints: Seq[LatencyDataPoint], 
                     throughput: Int): LatencyMetrics =
  val latencies = dataPoints.map(_.latencyMs).sorted
```
- **Extract latencies:** Get just the latency values from all data points
- **Sort ascending:** Required for percentile calculations
- **Example:** `[5000.0, 5500.0, 6000.0, 6200.0, 12000.0]` ms

**Basic statistics:**

```scala
val count = latencies.size
val sum = latencies.sum
val avg = sum / count
val min = latencies.head
val max = latencies.last
```
- **Count:** Number of windows analyzed (e.g., 14)
- **Sum:** Total of all latencies
- **Average (mean):** `sum / count` (e.g., 5595.57 ms)
- **Min:** First value in sorted list (lowest latency)
- **Max:** Last value in sorted list (highest latency)

**Percentile calculation:**

```scala
def percentile(p: Double): Double =
  val index = (count * p).toInt
  latencies(Math.min(index, count - 1))
```
- **Non-parametric percentile:** Find the value at a specific position in the sorted array
- **Formula:** `index = floor(count × p)`
- **Examples:**
  - **P50 (median):** `index = 14 × 0.50 = 7` → 7th value (middle)
  - **P95:** `index = 14 × 0.95 = 13` → 13th value (only 1 value is higher)
  - **P99:** `index = 14 × 0.99 = 13` → 13th value (almost the highest)
- **`Math.min(index, count - 1)`:** Prevents index out of bounds (caps at last element)

```scala
val p50 = percentile(0.50)  // Median
val p95 = percentile(0.95)
val p99 = percentile(0.99)
```
- **Calculate specific percentiles:**
  - **P50 (median):** Middle value—50% of windows have lower latency
  - **P95:** 95% of windows have latency below this value
  - **P99:** 99% of windows have latency below this value

**Standard deviation:**

```scala
val variance = latencies.map(l => pow(l - avg, 2)).sum / count
val stdDev = sqrt(variance)
```
- **Variance calculation:**
  1. For each latency: compute `(latency - average)²` (squared difference from mean)
  2. Sum all squared differences
  3. Divide by count → variance
- **Standard deviation:** Square root of variance
- **Interpretation:**
  - **Low std dev (< 3000 ms):** Consistent, predictable latency
  - **High std dev (> 5000 ms):** High variability, unpredictable performance
- **Example:** If avg = 6000 ms and stdDev = 3000 ms, most latencies fall between 3000-9000 ms (avg ± 1 std dev)

**Return metrics object:**

```scala
LatencyMetrics(
  avgLatencyMs = avg, p50LatencyMs = p50, p95LatencyMs = p95, 
  p99LatencyMs = p99, minLatencyMs = min, maxLatencyMs = max, 
  stdDevLatencyMs = stdDev, sampleCount = count, 
  testThroughput = throughput, 
  timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
)
```
- **Package all metrics:** Create a single object containing all computed statistics
- **Additional fields:**
  - `sampleCount` - Number of windows analyzed
  - `testThroughput` - Target throughput for this test run
  - `timestamp` - When the analysis was performed (ISO format: `"2025-10-21T15:30:45"`)

---

## Complete Data Flow Example

Let's trace a single message through the entire pipeline:

### 1. Producer (t=0 ms)
```scala
val producerTimestamp = System.currentTimeMillis()  // 1729468800000
record.put("producer_ts", producerTimestamp)
producer.send(producerRecord)
```
**Result:** Message sent to Kafka with `producer_ts = 1729468800000`

### 2. Kafka (t=5 ms)
- Message stored in partition
- Replicated to followers
- Acknowledgment sent to producer

### 3. Spark Consumer (t=10 ms - 62,000 ms)
```scala
// Read from Kafka
val kafkaDF = spark.readStream.format("kafka")...

// Deserialize Avro
strippedDF.select(from_avro($"avro_value", avroSchema))

// Wait for window to close (up to 60 seconds)
.groupBy(window(col("processing_time"), "1 minute"))

// Aggregate
.agg(min("producer_ts").as("min_producer_ts"))  // = 1729468800000

// Capture completion time
(unix_timestamp(current_timestamp()) * 1000).as("processing_end_ts")  // = 1729468862000

// Write to output topic
kafkaDF.writeStream.format("kafka").option("topic", "weather.aggregated.output")
```
**Result:** Aggregated record written to output topic at t=62,000 ms

### 4. Latency Monitor (t=62,005 ms)
```scala
// Read aggregated result
val minProducerTs = avroRecord.get("min_producer_ts")  // 1729468800000
val processingEndTs = avroRecord.get("processing_end_ts")  // 1729468862000

// Calculate latency
val latencyMs = processingEndTs - minProducerTs  // 62,000 ms = 62 seconds

// Validate and store
if latencyValid then dataPoints += LatencyDataPoint(...)
```

### 5. Statistical Analysis
```scala
val latencies = [5000, 5500, 6000, ..., 62000].sorted
val avg = 5595.57 ms
val p95 = 11996.00 ms
val p99 = 11996.00 ms
```
**Result:** Report showing average latency of ~5.6 seconds with P99 at ~12 seconds

---

## Key Takeaways

1. **Producer timestamps enable latency measurement:** By capturing `producer_ts` at message creation, we can measure end-to-end pipeline latency

2. **Spark optimizations reduce overhead:** Configurations like reduced shuffle partitions, AQE, and Kryo serialization minimize processing latency

3. **Processing-time windowing simplifies benchmarking:** Using arrival time instead of event time reduces complexity and focuses on pipeline performance

4. **Percentiles reveal tail latency:** P95 and P99 metrics show worst-case behavior that averages might hide

5. **Validation prevents garbage data:** Timestamp and latency bounds checks ensure statistical analysis is based on valid samples

This benchmark provides a comprehensive view of streaming pipeline performance, from message ingestion through aggregation to final latency analysis.