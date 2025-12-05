import os
import re
import threading
import time
import traceback
import requests
from datetime import datetime, timedelta
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, min as spark_min, max as spark_max
from pyspark.sql.avro.functions import from_avro

# NOTE: Assuming enrichers.py and schemas.py are available in the environment
from enrichers import (
    init_municipality_lookup,
    add_dk_area_udf,
    add_municipality_code_udf
)
from schemas import WIND_ENRICHED_SCHEMA, TEMP_ENRICHED_SCHEMA, SUN_ENRICHED_SCHEMA

# Input topics
INPUT_TOPICS = ["weather-wind", "weather-temp", "weather-sun"]

# Global Schema Registry Client
schema_registry_client = None

# Global tracking of current forecastId per topic
current_forecast_ids = {}


def get_latest_schema(registry_url, topic):
    subject = f"{topic}-value"
    url = f"{registry_url}/subjects/{subject}/versions/latest"
    try:
        response = requests.get(url)
        response.raise_for_status()
        schema_json = response.json().get("schema")
        print(f"Successfully fetched schema for {subject}")
        return schema_json
    except Exception as e:
        print(f"Failed to fetch schema for {subject}: {e}")
        raise e


def parse_step_to_hours(step_str):
    """Parse ISO 8601 duration to hours."""
    try:
        match = re.match(r'PT(\d+)H', step_str)
        if match:
            return int(match.group(1))
        return 0
    except Exception:
        return 0


def calculate_forecast_range(batch_df, batch_time):
    """Calculate forecast range from step field."""
    try:
        step_stats = batch_df.select(
            spark_min("step").alias("min_step"),
            spark_max("step").alias("max_step")
        ).collect()[0]

        min_step_str = step_stats["min_step"] or "PT0H"
        max_step_str = step_stats["max_step"] or "PT72H"

        min_hours = parse_step_to_hours(min_step_str)
        max_hours = parse_step_to_hours(max_step_str)

        from_time = batch_time + timedelta(hours=min_hours)
        to_time = batch_time + timedelta(hours=max_hours)

        from_str = from_time.strftime("%Y-%m-%d_%H-%M")
        to_str = to_time.strftime("%Y-%m-%d_%H-%M")

        return from_time, to_time, from_str, to_str

    except Exception as e:
        print(f"Warning: Could not calculate forecast range: {e}")
        from_time = batch_time
        to_time = batch_time + timedelta(hours=72)
        from_str = from_time.strftime("%Y-%m-%d_%H-%M")
        to_str = to_time.strftime("%Y-%m-%d_%H-%M")
        return from_time, to_time, from_str, to_str


def delete_hdfs_path(spark, hdfs_path):
    """Delete HDFS path."""
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)

        if fs.exists(path):
            fs.delete(path, True)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Deleted: {hdfs_path}")
            return True
        return False
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Warning: Could not delete {hdfs_path}: {e}")
        return False


def copy_hdfs_file(spark, source, destination):
    """Copy HDFS file. (Used for copying contents of a directory)"""
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

        src_path = spark._jvm.org.apache.hadoop.fs.Path(source)
        dst_path = spark._jvm.org.apache.hadoop.fs.Path(destination)

        if not fs.exists(src_path):
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Source doesn't exist: {source}")
            return False

        # Create destination directory if needed
        dst_parent = dst_path.getParent()
        if not fs.exists(dst_parent):
            fs.mkdirs(dst_parent)

        # Use Spark's FileUtil (DFSUtil) for directory-level copy if available/reliable.
        org_FileUtil = spark._jvm.org.apache.hadoop.fs.FileUtil
        # Setting the `deleteSource` flag to False
        org_FileUtil.copy(fs, src_path, fs, dst_path, False, hadoop_conf)

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Copied: {source} → {destination}")
        return True

    except Exception as e:
        # A common failure here is trying to copy a directory like a single file.
        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Error copying path {source} to {destination}: {e}")
        return False


def restore_current_forecast_id(spark, live_path):
    """
    Restore forecastId from existing live data directory.
    Returns None if the directory is empty or column is missing.
    """
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(live_path)

        # Check if the path exists and is not empty
        if not fs.exists(path) or fs.listStatus(path).length == 0:
            return None

        # Read the Avro data from the directory (Spark handles part files)
        df = spark.read.format("avro").load(live_path)

        # Check if 'forecastId' column exists in the schema
        if "forecastId" not in df.columns:
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Found existing data, but 'forecastId' column is missing.")
            return None

        forecast_id = df.select("forecastId").first()[0]
        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Restored forecastId from live data: {forecast_id[:8]}...")
        return forecast_id

    except Exception as e:
        # This catches Py4JJavaError (e.g., if it can't read the files due to schema mismatch)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Could not restore forecastId: {e}")
        return None


def archive_live_file(spark, topic, live_path, forecast_id, hdfs_namenode):
    """Archive current live directory contents to historical when new forecast cycle starts."""
    try:
        timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # Build historical archive path
        year = datetime.now().strftime("%Y")
        month = datetime.now().strftime("%m")
        # Archive to a directory named with the forecastId and timestamp
        archive_dir_name = f"{topic}_{datetime.now().strftime('%Y-%m-%d_%H-%M')}_{forecast_id[:8]}"
        historical_path = f"{hdfs_namenode}/historical/live-archives/{year}/{month}/{archive_dir_name}"

        print(f"[{timestamp_str}] 📦 Archiving live directory to historical...")
        print(f"[{timestamp_str}]    From: {live_path}")
        print(f"[{timestamp_str}]    To: {historical_path}")

        # 1. Read the current live data
        live_df = spark.read.format("avro").load(live_path)

        # 2. Write it to the historical archive path
        live_df.write \
            .mode("overwrite") \
            .format("avro") \
            .save(historical_path)

        print(f"[{timestamp_str}] ✅ Archive completed: {historical_path}")
        return True

    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Error archiving live data: {e}")
        traceback.print_exc()
        return False


def monitor_progress(query):
    def run():
        time.sleep(5)
        while query.isActive:
            try:
                time.sleep(30)
                progress = query.lastProgress
                if progress:
                    print("=" * 60)
                    print("STREAM PROGRESS:", query.name)
                    print("Status:", query.status['message'])
                    print("Input rows:", progress.get('numInputRows', 0))
                    print("Batch ID:", progress.get('batchId'))
                    print("=" * 60)
            except Exception:
                break

    threading.Thread(target=run, daemon=True).start()


def create_stream_for_topic(spark, topic: str, avro_schema_registry_url: str, checkpoint_root: str,
                            trigger_interval="10 seconds"):
    print(f"Creating stream for topic: {topic}")

    # Fetch schema
    input_schema_json = get_latest_schema(avro_schema_registry_url, topic)

    # Determine output configs
    if topic == "weather-wind":
        out_topic = "weather-wind-enriched"
        out_schema_str = WIND_ENRICHED_SCHEMA
    elif topic == "weather-temp":
        out_topic = "weather-temp-enriched"
        out_schema_str = TEMP_ENRICHED_SCHEMA
    elif topic == "weather-sun":
        out_topic = "weather-sun-enriched"
        out_schema_str = SUN_ENRICHED_SCHEMA
    else:
        raise ValueError(f"Unsupported topic: {topic}")

    # HDFS paths
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000").rstrip("/")
    live_dir_base = f"{hdfs_namenode}/live/forecast"
    # FIX 1: live_path must be a directory for Spark append mode to work.
    live_path = f"{live_dir_base}/{topic}"
    historical_base = f"{hdfs_namenode}/historical"

    # Try to restore current forecastId from live data directory
    if topic not in current_forecast_ids:
        # FIX 2: Pass the correct directory path to restore function
        current_forecast_ids[topic] = restore_current_forecast_id(spark, live_path)

    # Read stream
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("BOOTSTRAP_SERVERS", "kafka-g5:9092"))
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "100000")
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "30000")
        .load()
    )

    # Deserialize & enrich
    # Kafka uses a 5-byte header before the Avro payload (magic byte + 4-byte schema ID)
    # The subscription is to the original topic name, e.g., "weather-wind"
    parsed = df.select(
        from_avro(expr("substring(value, 6)"), input_schema_json, {"mode": "PERMISSIVE"}).alias("data"),
        col("timestamp")
    )

    parsed = parsed.filter(col("data").isNotNull())

    flat = parsed.select(
        col("data.lon").alias("lon"),
        col("data.lat").alias("lat"),
        col("data.value").alias("value"),
        col("data.step").alias("step"),
        col("data.parameter").alias("parameter"),
        col("data.forecastId").alias("forecastId"),  # NEW
        col("timestamp")
    )

    enriched_df = (
        flat
        .withColumn("dkArea", add_dk_area_udf(col("lon")))
        .withColumn("municipalityCode", add_municipality_code_udf(col("lat"), col("lon")))
    )

    # Kafka serializers
    avro_serializer = AvroSerializer(schema_registry_client, out_schema_str)
    string_serializer = StringSerializer('utf_8')
    producer_conf = {'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS", "kafka-g5:9092")}

    # foreachBatch logic WITH forecastId tracking
    def write_to_kafka_live_and_historical(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        batch_start_time = datetime.now()
        timestamp_str = batch_start_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        try:
            # Get forecastId from this batch
            batch_forecast_id = batch_df.select("forecastId").first()[0]

            # Check if this is a NEW forecast cycle
            # Use 'if current_forecast_ids.get(topic)' to simplify the logic, but the explicit
            # None check is safer since we set it to None on failure.
            if current_forecast_ids[topic] is None:
                # First batch ever OR restart after empty HDFS (None restored)
                print("\n" + "=" * 80)
                print(f"[{timestamp_str}] 🆕 FIRST FORECAST CYCLE (or restart after data loss)")
                print(f"[{timestamp_str}] Forecast ID: {batch_forecast_id[:8]}...")
                print("=" * 80)
                current_forecast_ids[topic] = batch_forecast_id

            elif batch_forecast_id != current_forecast_ids[topic]:
                # NEW FORECAST CYCLE DETECTED!
                print("\n" + "=" * 80)
                print(f"[{timestamp_str}] 🔄 NEW FORECAST CYCLE DETECTED!")
                print("=" * 80)
                print(f"[{timestamp_str}] Old Forecast ID: {current_forecast_ids[topic][:8]}...")
                print(f"[{timestamp_str}] New Forecast ID: {batch_forecast_id[:8]}...")
                print("=" * 80)

                # FIX 3: Pass the correct directory path to archive function
                archive_live_file(spark, topic, live_path, current_forecast_ids[topic], hdfs_namenode)

                # FIX 4: Delete old live data directory
                delete_hdfs_path(spark, live_path)

                # Update tracking
                current_forecast_ids[topic] = batch_forecast_id

                print(f"[{timestamp_str}] ✅ Ready for new forecast cycle")
                print("=" * 80 + "\n")

            print("\n" + "=" * 80)
            print(f"[{timestamp_str}] ========== BATCH {batch_id} START ({topic}) ==========")
            print(f"[{timestamp_str}] Forecast ID: {batch_forecast_id[:8]}...")
            print(f"[{timestamp_str}] Triple Sink: Kafka → Live HDFS (DIR: {topic}) → Historical HDFS")
            print("=" * 80)

            record_count = batch_df.count()
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Received {record_count:,} records")

            # Calculate forecast range
            from_time, to_time, from_str, to_str = calculate_forecast_range(batch_df, batch_start_time)
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Forecast range: {from_str} to {to_str}")

            # Sink 1: Kafka Enriched Topics
            kafka_start = datetime.now()
            print(
                f"\n[{kafka_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] 📤 SINK 1/3: Writing to Kafka enriched topic: {out_topic}")

            # Collect is safe here as batch is max 100k, but generally not scalable for huge batches
            rows = batch_df.collect()
            producer = SerializingProducer(producer_conf)

            kafka_sent_count = 0
            for row in rows:
                try:
                    record = row.asDict()
                    key_str = f"{record['lat']}_{record['lon']}" if record['lat'] and record['lon'] else "unknown_loc"
                    producer.produce(
                        topic=out_topic,
                        key=string_serializer(key_str, SerializationContext(out_topic, MessageField.KEY)),
                        value=avro_serializer(record, SerializationContext(out_topic, MessageField.VALUE))
                    )
                    kafka_sent_count += 1
                except Exception as inner_e:
                    print(f"Skipping bad record: {inner_e}")

            producer.flush()
            kafka_duration = (datetime.now() - kafka_start).total_seconds()
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] ✅ Kafka enriched: {kafka_sent_count:,} records → '{out_topic}' ({kafka_duration:.2f}s)")

            # Sink 2: Live File (APPEND to accumulate batches for current forecast cycle)
            live_start = datetime.now()
            # Use the directory path for Spark save
            print(
                f"\n[{live_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] 💾 SINK 2/3: Appending to live HDFS: {live_path}")

            batch_df.write \
                .mode("append") \
                .format("avro") \
                .save(live_path)

            live_duration = (datetime.now() - live_start).total_seconds()
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] ✅ Live HDFS: {record_count:,} records appended ({live_duration:.2f}s)")

            # Sink 3: Historical Batches (as before, for detailed tracking)
            historical_start = datetime.now()
            year = batch_start_time.strftime("%Y")
            month = batch_start_time.strftime("%m")
            # This path is already a unique directory (good)
            batch_dir_name = f"{from_str}_to_{to_str}_batch-{batch_id}_forecast-{batch_forecast_id[:8]}"
            historical_path = f"{historical_base}/{year}/{topic}/{month}/{batch_dir_name}"

            print(
                f"\n[{historical_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] 📁 SINK 3/3: Writing to historical HDFS: {historical_path}")

            # Use overwrite mode for historical batches since the path is unique per batch/ID
            batch_df.write \
                .mode("overwrite") \
                .format("avro") \
                .save(historical_path)

            historical_duration = (datetime.now() - historical_start).total_seconds()
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] ✅ Historical HDFS: {record_count:,} records archived ({historical_duration:.2f}s)")

            # Summary
            total_duration = (datetime.now() - batch_start_time).total_seconds()
            print("\n" + "=" * 80)
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] ✅ BATCH {batch_id} COMPLETED in {total_duration:.2f}s")
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}]    ├─ Kafka enriched: {kafka_sent_count:,} records")
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}]    ├─ Live HDFS: appended")
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}]    └─ Historical HDFS: archived")
            print("=" * 80 + "\n")

        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Error in batch {batch_id}: {e}")
            traceback.print_exc()

    # Start stream
    checkpoint_location = f"{checkpoint_root}/{topic.replace('/', '_')}_chkpt"

    query = (
        enriched_df.writeStream
        .foreachBatch(write_to_kafka_live_and_historical)
        .queryName(f"Enricher_{topic}")
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    # FIX 6: Use the correct live path in the print statement
    print(f"Started stream: {topic} -> Kafka({out_topic}) & Live({live_path}) & Historical({historical_base})")
    return query


def main():
    global schema_registry_client

    # Configuration
    checkpoint_root = os.getenv("CHECKPOINT_ROOT", "/tmp/spark/checkpoints/kafka_enricher_v5")
    trigger_interval = os.getenv("TRIGGER_INTERVAL", "30 seconds")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    municipality_csv_hdfs = os.getenv("MUNICIPALITY_CSV_HDFS",
                                      "hdfs://namenode-g5:9000/utils/municipality_codes_to_coordinates.csv")
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000")

    schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})

    spark = (
        SparkSession.builder
        .appName("KafkaWeatherEnricher")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.hadoop.fs.defaultFS", hdfs_namenode)
        .config("spark.speculation", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    import logging
    logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.sql.kafka010").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.sql.execution.streaming").setLevel(logging.ERROR)

    # Initialize municipality lookup
    init_municipality_lookup(spark, municipality_csv_hdfs)

    queries = []
    try:
        for t in INPUT_TOPICS:
            q = create_stream_for_topic(spark, t, schema_registry_url, checkpoint_root, trigger_interval)
            monitor_progress(q)
            queries.append(q)

            print("Waiting 15 seconds before starting next stream...")
            time.sleep(15)

        print(f"All streams started. Checkpoint: {checkpoint_root}")
        for q in queries:
            q.awaitTermination()

    except Exception as e:
        print("Error in streaming job:")
        traceback.print_exc()
        time.sleep(10)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()