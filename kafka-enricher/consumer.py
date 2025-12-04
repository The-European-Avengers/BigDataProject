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

from enrichers import (
    init_municipality_lookup,
    add_dk_area_udf,
    add_municipality_code_udf
)
from schemas import WIND_ENRICHED_SCHEMA, TEMP_ENRICHED_SCHEMA, SUN_ENRICHED_SCHEMA

# Input topics
INPUT_TOPICS = ["weather-wind", "weather-temp", "weather-sun"]

# Global Schema Registry Client (Initialized in main)
schema_registry_client = None


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
    """
    Parse ISO 8601 duration string (e.g., 'PT0H', 'PT72H') to hours.
    Returns integer hours or 0 if parsing fails.
    """
    try:
        match = re.match(r'PT(\d+)H', step_str)
        if match:
            return int(match.group(1))
        return 0
    except Exception:
        return 0


def calculate_forecast_range(batch_df, batch_time):
    """
    Calculate forecast range from step field.
    Returns tuple: (from_datetime, to_datetime, from_str, to_str)
    """
    try:
        # Get min and max step values
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
        # Fallback: assume 0 to 72 hours
        from_time = batch_time
        to_time = batch_time + timedelta(hours=72)
        from_str = from_time.strftime("%Y-%m-%d_%H-%M")
        to_str = to_time.strftime("%Y-%m-%d_%H-%M")
        return from_time, to_time, from_str, to_str


def delete_hdfs_path(spark, hdfs_path):
    """
    Delete HDFS path using Hadoop FileSystem API via Spark.
    """
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)

        if fs.exists(path):
            fs.delete(path, True)  # True = recursive
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Deleted old path: {hdfs_path}")
            return True
        else:
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Path does not exist (skip delete): {hdfs_path}")
            return False
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Warning: Could not delete {hdfs_path}: {e}")
        return False


def rename_to_single_file(spark, source_dir, target_file):
    """
    Rename the single part file from coalesce(1) to a clean filename.
    Example: part-00000-*.avro -> weather-wind.avro
    """
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

        source_path = spark._jvm.org.apache.hadoop.fs.Path(source_dir)

        # Find the part file
        status_list = fs.listStatus(source_path)
        part_file = None

        for status in status_list:
            filename = status.getPath().getName()
            if filename.startswith("part-") and filename.endswith(".avro"):
                part_file = status.getPath()
                break

        if part_file is None:
            raise Exception(f"No part file found in {source_dir}")

        # Rename to target
        target_path = spark._jvm.org.apache.hadoop.fs.Path(target_file)
        fs.rename(part_file, target_path)

        # Clean up temp directory
        fs.delete(source_path, True)

        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Renamed {part_file.getName()} -> {target_file}")
        return True

    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Error renaming file: {e}")
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

    # 1. Fetch Input Schema dynamically
    input_schema_json = get_latest_schema(avro_schema_registry_url, topic)

    # 2. Determine Output Configs
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

    # Construct HDFS paths
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000")
    hdfs_namenode = hdfs_namenode.rstrip("/")

    # Live path (single file)
    live_dir = f"{hdfs_namenode}/live/forecast"
    live_file = f"{live_dir}/{topic}.avro"
    live_temp_dir = f"{live_dir}/.temp_{topic}"

    # Historical path (multiple parts, organized by year/month)
    historical_base = f"{hdfs_namenode}/historical"

    # 3. Read Stream
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

    # 4. Deserialize & Enrich
    parsed = df.select(
        from_avro(
            expr("substring(value, 6)"),
            input_schema_json,
            {"mode": "PERMISSIVE"}
        ).alias("data"),
        col("timestamp")
    )

    parsed = parsed.filter(col("data").isNotNull())

    flat = parsed.select(
        col("data.lon").alias("lon"),
        col("data.lat").alias("lat"),
        col("data.value").alias("value"),
        col("data.step").alias("step"),
        col("data.parameter").alias("parameter"),
        col("timestamp")
    )

    enriched_df = (
        flat
        .withColumn("dkArea", add_dk_area_udf(col("lon")))
        .withColumn("municipalityCode", add_municipality_code_udf(col("lat"), col("lon")))
    )

    # Prepare Kafka serializers
    avro_serializer = AvroSerializer(schema_registry_client, out_schema_str)
    string_serializer = StringSerializer('utf_8')
    producer_conf = {'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS", "kafka-g5:9092")}

    # 5. Define foreachBatch Logic (Triple Sink: Kafka + Live + Historical)
    def write_to_kafka_live_and_historical(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        batch_start_time = datetime.now()
        timestamp_str = batch_start_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        print("\n" + "=" * 80)
        print(f"[{timestamp_str}] ========== BATCH {batch_id} START ({topic}) ==========")
        print("=" * 80)

        try:
            # Count records
            record_count = batch_df.count()
            timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"[{timestamp_str}] Received {record_count:,} records from Kafka")

            # Calculate forecast range
            from_time, to_time, from_str, to_str = calculate_forecast_range(batch_df, batch_start_time)
            timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"[{timestamp_str}] Forecast range: {from_str} to {to_str}")

            # --- ENRICHMENT (already done in stream transformation) ---
            timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"[{timestamp_str}] Enrichment completed (dkArea + municipalityCode)")

            # --- Sink 1: Kafka Enriched Topics ---
            kafka_start = datetime.now()
            timestamp_str = kafka_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"[{timestamp_str}] Writing to Kafka topic: {out_topic}")

            rows = batch_df.collect()
            producer = SerializingProducer(producer_conf)

            for row in rows:
                try:
                    record = row.asDict()
                    if record['lat'] is not None and record['lon'] is not None:
                        key_str = f"{record['lat']}_{record['lon']}"
                    else:
                        key_str = "unknown_loc"

                    producer.produce(
                        topic=out_topic,
                        key=string_serializer(key_str, SerializationContext(out_topic, MessageField.KEY)),
                        value=avro_serializer(record, SerializationContext(out_topic, MessageField.VALUE))
                    )
                except Exception as inner_e:
                    print(f"Skipping bad record in batch {batch_id}: {inner_e}")

            producer.flush()
            kafka_duration = (datetime.now() - kafka_start).total_seconds()
            timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"[{timestamp_str}] Kafka write completed ({record_count:,} records sent in {kafka_duration:.2f}s)")

            # --- Sink 2: Live Folder (Single File, Overwrite) ---
            live_start = datetime.now()
            timestamp_str = live_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"[{timestamp_str}] Writing to /live/forecast/{topic}.avro (OVERWRITE)")

            # Delete old live file/directory
            delete_hdfs_path(spark, live_file)
            delete_hdfs_path(spark, live_temp_dir)

            # Write to temp location with coalesce(1)
            timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"[{timestamp_str}] Writing new /live/forecast/{topic}.avro")

            batch_df.coalesce(1).write \
                .mode("overwrite") \
                .format("avro") \
                .save(live_temp_dir)

            # Rename to clean filename
            rename_to_single_file(spark, live_temp_dir, live_file)

            live_duration = (datetime.now() - live_start).total_seconds()
            timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"[{timestamp_str}] Live file write completed in {live_duration:.2f}s")

            # --- Sink 3: Historical Folder (Multiple Parts, Append) ---
            historical_start = datetime.now()

            # Build historical path: /historical/YYYY/weather-{topic}/MM/from_to_batch/
            year = batch_start_time.strftime("%Y")
            month = batch_start_time.strftime("%m")
            batch_dir_name = f"{from_str}_to_{to_str}_batch-{batch_id}"

            historical_path = f"{historical_base}/{year}/{topic}/{month}/{batch_dir_name}"

            timestamp_str = historical_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"[{timestamp_str}] Writing to {historical_path}")

            # Write with multiple partitions (default Spark behavior for large data)
            batch_df.write \
                .mode("append") \
                .format("avro") \
                .save(historical_path)

            historical_duration = (datetime.now() - historical_start).total_seconds()
            timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"[{timestamp_str}] Historical write completed in {historical_duration:.2f}s")

            # --- Summary ---
            total_duration = (datetime.now() - batch_start_time).total_seconds()
            timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print("=" * 80)
            print(f"[{timestamp_str}] ========== BATCH {batch_id} COMPLETED in {total_duration:.2f}s ==========")
            print("=" * 80 + "\n")

        except Exception as e:
            timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"[{timestamp_str}] Error in batch {batch_id} for {topic}: {e}")
            traceback.print_exc()

    # 6. Start Stream
    checkpoint_location = f"{checkpoint_root}/{topic.replace('/', '_')}_chkpt"

    query = (
        enriched_df.writeStream
        .foreachBatch(write_to_kafka_live_and_historical)
        .queryName(f"Enricher_{topic}")
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    print(f"Started stream: {topic} -> Kafka({out_topic}) & Live({live_file}) & Historical({historical_base})")
    return query


def main():
    global schema_registry_client

    # Load Envs
    checkpoint_root = os.getenv("CHECKPOINT_ROOT", "/tmp/spark/checkpoints/kafka_enricher")
    trigger_interval = os.getenv("TRIGGER_INTERVAL", "30 seconds")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    municipality_csv_hdfs = os.getenv("MUNICIPALITY_CSV_HDFS",
                                      "hdfs://namenode-g5:9000/utils/municipality_codes_to_coordinates.csv")
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000")

    # Initialize Schema Registry Client
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

    # Suppress logs
    spark.sparkContext.setLogLevel("ERROR")

    import logging
    logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.sql.kafka010").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.sql.execution.streaming").setLevel(logging.ERROR)

    # Init Lookup
    init_municipality_lookup(spark, municipality_csv_hdfs)

    queries = []
    try:
        # Start streams staggered
        for t in INPUT_TOPICS:
            q = create_stream_for_topic(spark, t, schema_registry_url, checkpoint_root, trigger_interval)
            monitor_progress(q)
            queries.append(q)

            print("Waiting 15 seconds before starting next stream to balance load...")
            time.sleep(15)

        print(f"All streams started. Checkpoint dir: {checkpoint_root}")
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