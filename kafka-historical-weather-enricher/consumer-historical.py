#!/usr/bin/env python3
"""
Historical Weather Data Spark Streaming Consumer
Reads historical weather data from Kafka, enriches with dkArea and municipalityCode,
and saves to HDFS in monthly Avro files with deduplication.
"""

import os
import time
import traceback
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, to_timestamp,
    row_number, lit, expr
)
from pyspark.sql.window import Window
from pyspark.sql.avro.functions import from_avro
import requests

# Import enrichment functions
from enrichers import (
    init_municipality_lookup,
    add_dk_area_udf,
    add_municipality_code_udf
)

# Input topics for historical weather data
INPUT_TOPICS = [
    "historical-weather-wind",
    "historical-weather-temp",
    "historical-weather-sun"
]

# Topic to parameter name mapping
TOPIC_TO_PARAM = {
    "historical-weather-wind": "mean_wind_speed",
    "historical-weather-temp": "mean_temp",
    "historical-weather-sun": "mean_radiation"
}

# Topic to weather type mapping for HDFS paths
TOPIC_TO_TYPE = {
    "historical-weather-wind": "weather-wind",
    "historical-weather-temp": "weather-temp",
    "historical-weather-sun": "weather-sun"
}


def get_latest_schema(registry_url, topic):
    """Fetch the latest Avro schema for a topic from Schema Registry."""
    subject = f"{topic}-value"
    url = f"{registry_url}/subjects/{subject}/versions/latest"
    try:
        response = requests.get(url)
        response.raise_for_status()
        schema_json = response.json().get("schema")
        print(f"✓ Successfully fetched schema for {subject}")
        return schema_json
    except Exception as e:
        print(f"✗ Failed to fetch schema for {subject}: {e}")
        raise e


def read_existing_monthly_data(spark, hdfs_path):
    """
    Read existing monthly Avro file if it exists.
    Returns DataFrame or None if file doesn't exist.
    """
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)

        if fs.exists(path):
            print(f"  📂 Found existing data at {hdfs_path}")
            df = spark.read.format("avro").load(hdfs_path)
            record_count = df.count()
            print(f"  📊 Existing records: {record_count:,}")
            return df
        else:
            print(f"  📂 No existing data at {hdfs_path}")
            return None

    except Exception as e:
        print(f"  ⚠️  Could not read existing data: {e}")
        return None


def delete_hdfs_path(spark, hdfs_path):
    """Delete HDFS path."""
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)

        if fs.exists(path):
            fs.delete(path, True)
            print(f"  🗑️  Deleted: {hdfs_path}")
            return True
        return False
    except Exception as e:
        print(f"  ⚠️  Warning: Could not delete {hdfs_path}: {e}")
        return False


def create_stream_for_topic(spark, topic, schema_registry_url, checkpoint_root, trigger_interval, hdfs_namenode):
    """
    Create a streaming query for a historical weather topic.
    Enriches data and saves to monthly HDFS files.
    """

    timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print("\n" + "=" * 80)
    print(f"[{timestamp_str}] 🚀 STARTING STREAM FOR: {topic}")
    print("=" * 80)

    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "kafka-g5:9092")

    # Get schema from registry
    schema_str = get_latest_schema(schema_registry_url, topic)

    # Read from Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # Read from beginning of topic
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "50000")
        .load()
    )

    # Deserialize Avro
    # CRITICAL FIX: Strip the 5-byte Confluent Schema Registry header
    # Bytes 0: magic byte (0x00)
    # Bytes 1-4: schema ID (big-endian int)
    # Bytes 5+: actual Avro data
    decoded_df = raw_df.select(
        from_avro(
            expr("substring(value, 6, length(value)-5)"),  # Skip first 5 bytes
            schema_str
        ).alias("data")
    ).select("data.*")

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] ✓ Stream configured for {topic}")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}]   Schema: {decoded_df.schema.simpleString()}")

    # Enrich with dkArea and municipalityCode
    enriched_df = decoded_df \
        .withColumn("dkArea", add_dk_area_udf(col("lon"))) \
        .withColumn("municipalityCode", add_municipality_code_udf(col("lat"), col("lon")))

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] ✓ Enrichment UDFs applied")

    # Get parameter name for this topic
    param_name = TOPIC_TO_PARAM.get(topic, "value")
    weather_type = TOPIC_TO_TYPE.get(topic, topic)

    def write_to_hdfs(batch_df, batch_id):
        """
        Process each micro-batch:
        1. Parse timeObserved to extract year/month
        2. Group by year/month
        3. For each year/month, merge with existing data and deduplicate
        4. Write back to HDFS
        """
        if batch_df.isEmpty():
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] ⏭️  Batch {batch_id}: Empty, skipping")
            return

        try:
            batch_start_time = datetime.now()
            timestamp_str = batch_start_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

            print("\n" + "=" * 80)
            print(f"[{timestamp_str}] ========== BATCH {batch_id} START ({topic}) ==========")
            print("=" * 80)

            record_count = batch_df.count()
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] 📥 Received {record_count:,} records")

            # Convert timeObserved to timestamp and extract year/month
            batch_with_time = batch_df.withColumn(
                "timestamp", to_timestamp(col("timeObserved"))
            ).withColumn(
                "year", year(col("timestamp"))
            ).withColumn(
                "month", month(col("timestamp"))
            )

            # Get unique year/month combinations in this batch
            year_months = batch_with_time.select("year", "month").distinct().collect()

            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] 📅 Processing {len(year_months)} year/month combination(s)")

            total_written = 0

            for ym in year_months:
                year_val = ym["year"]
                month_val = ym["month"]
                month_str = f"{month_val:02d}"

                print(
                    f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] 📁 Processing {year_val}-{month_str}...")

                # Filter batch data for this year/month
                monthly_new_data = batch_with_time.filter(
                    (col("year") == year_val) & (col("month") == month_val)
                )

                new_record_count = monthly_new_data.count()
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}]   📊 New records: {new_record_count:,}")

                # Build HDFS path for this month
                hdfs_path = f"{hdfs_namenode}/historical/{year_val}/{weather_type}/{month_str}.avro"

                # Read existing data if available
                existing_df = read_existing_monthly_data(spark, hdfs_path)

                # Prepare new data - rename valueFieldName column to parameter name
                # Schema: timeObserved, stationId, stationName, <param_name>, lon, lat, dkArea, municipalityCode
                new_data_renamed = monthly_new_data.select(
                    col("timeObserved"),
                    col("stationId"),
                    col("stationName"),
                    col("value").alias(param_name),  # Rename 'value' to parameter name
                    col("lon"),
                    col("lat"),
                    col("dkArea"),
                    col("municipalityCode"),
                    col("batchId")  # Keep for deduplication
                )

                # Combine with existing data if present
                if existing_df is not None:
                    # Ensure existing data has batchId column (may be missing in old data)
                    if "batchId" not in existing_df.columns:
                        existing_df = existing_df.withColumn("batchId", lit(""))

                    combined_df = existing_df.union(new_data_renamed)
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}]   🔗 Combined with existing data")
                else:
                    combined_df = new_data_renamed
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}]   🆕 Creating new monthly file")

                # Deduplicate: Keep latest record for each (stationId, timeObserved)
                # Use row_number() to rank by batchId descending (latest batch wins)
                window_spec = Window.partitionBy("stationId", "timeObserved").orderBy(col("batchId").desc())

                deduplicated_df = combined_df \
                    .withColumn("row_num", row_number().over(window_spec)) \
                    .filter(col("row_num") == 1) \
                    .drop("row_num", "batchId")  # Remove helper columns

                # CRITICAL FIX: Cache the DataFrame to materialize it in memory
                # This prevents the race condition where we delete files that Spark still needs to read
                deduplicated_df = deduplicated_df.cache()

                # Trigger execution and cache the data (this reads from HDFS before we delete)
                final_count = deduplicated_df.count()
                print(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}]   ✓ After deduplication: {final_count:,} records")

                # Write to HDFS (overwrite the monthly file)
                write_start = datetime.now()

                # Now it's safe to delete - data is already materialized in memory
                delete_hdfs_path(spark, hdfs_path)

                # Write the deduplicated data (reads from cache, not disk)
                deduplicated_df \
                    .orderBy("timeObserved", "stationId") \
                    .write \
                    .mode("overwrite") \
                    .format("avro") \
                    .save(hdfs_path)

                # Unpersist to free memory
                deduplicated_df.unpersist()

                write_duration = (datetime.now() - write_start).total_seconds()
                print(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}]   💾 Written to HDFS: {hdfs_path} ({write_duration:.2f}s)")

                total_written += final_count

            # Summary
            total_duration = (datetime.now() - batch_start_time).total_seconds()
            print("\n" + "=" * 80)
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] ✅ BATCH {batch_id} COMPLETED in {total_duration:.2f}s")
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}]    └─ Total records in HDFS: {total_written:,}")
            print("=" * 80 + "\n")

        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] ✗ Error in batch {batch_id}: {e}")
            traceback.print_exc()

    # Start stream
    checkpoint_location = f"{checkpoint_root}/{topic.replace('/', '_')}_chkpt"

    query = (
        enriched_df.writeStream
        .foreachBatch(write_to_hdfs)
        .queryName(f"HistoricalEnricher_{topic}")
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] ✓ Stream started: {topic} -> HDFS {hdfs_namenode}/historical/")
    return query


def monitor_progress(query):
    """Monitor streaming query progress."""
    def monitor():
        while query.isActive:
            try:
                progress = query.lastProgress
                if progress:
                    timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    print(f"[{timestamp_str}] 📊 Query: {query.name}")
                    print(f"[{timestamp_str}]    ├─ Input Rate: {progress.get('inputRowsPerSecond', 0):.2f} rows/sec")
                    print(f"[{timestamp_str}]    ├─ Process Rate: {progress.get('processedRowsPerSecond', 0):.2f} rows/sec")
                    print(f"[{timestamp_str}]    └─ Batch Duration: {progress.get('batchDuration', 0)}ms")
            except Exception as e:
                print(f"Error monitoring progress: {e}")
            time.sleep(30)

    import threading
    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()


def main():
    """Main entry point for historical weather consumer."""

    # Configuration from environment
    checkpoint_root = os.getenv("CHECKPOINT_ROOT", "/tmp/spark/checkpoints/historical_enricher_v1")
    trigger_interval = os.getenv("TRIGGER_INTERVAL", "60 seconds")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    municipality_csv_hdfs = os.getenv("MUNICIPALITY_CSV_HDFS",
                                      "hdfs://namenode-g5:9000/utils/municipality_codes_to_coordinates.csv")
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000")

    print("=" * 80)
    print("HISTORICAL WEATHER DATA SPARK CONSUMER")
    print("=" * 80)
    print(f"Topics: {', '.join(INPUT_TOPICS)}")
    print(f"HDFS: {hdfs_namenode}/historical/<year>/<weather-type>/<month>.avro")
    print(f"Checkpoint: {checkpoint_root}")
    print(f"Trigger Interval: {trigger_interval}")
    print("=" * 80)

    # Initialize Spark
    spark = (
        SparkSession.builder
        .appName("HistoricalWeatherEnricher")
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

    # Suppress Kafka and streaming logs
    import logging
    logging.getLogger("org.apache.kafka").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.sql.kafka010").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.sql.execution.streaming").setLevel(logging.ERROR)

    # Initialize municipality lookup
    print("\n📍 Initializing municipality lookup...")
    init_municipality_lookup(spark, municipality_csv_hdfs)

    # Start streams for each topic
    queries = []
    try:
        for t in INPUT_TOPICS:
            q = create_stream_for_topic(spark, t, schema_registry_url, checkpoint_root, trigger_interval, hdfs_namenode)
            monitor_progress(q)
            queries.append(q)

            print(f"\n⏳ Waiting 15 seconds before starting next stream...\n")
            time.sleep(15)

        print("\n" + "=" * 80)
        print("✅ ALL STREAMS STARTED SUCCESSFULLY")
        print("=" * 80)
        print(f"Checkpoint location: {checkpoint_root}")
        print(f"Active streams: {len(queries)}")
        print("=" * 80 + "\n")

        # Wait for all streams
        for q in queries:
            q.awaitTermination()

    except Exception as e:
        print("\n" + "=" * 80)
        print("✗ ERROR IN STREAMING JOB")
        print("=" * 80)
        traceback.print_exc()
        print("=" * 80)
        time.sleep(10)
    finally:
        print("\n🛑 Stopping Spark session...")
        spark.stop()


if __name__ == "__main__":
    main()