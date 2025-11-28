# consumer.py
import os
import threading
import time
import traceback
import json
import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

from pyspark.sql import SparkSession
# ✅ FIXED IMPORTS
from pyspark.sql.functions import col, expr
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
        hdfs_subdir = "wind"
        out_schema_str = WIND_ENRICHED_SCHEMA
    elif topic == "weather-temp":
        out_topic = "weather-temp-enriched"
        hdfs_subdir = "temp"
        out_schema_str = TEMP_ENRICHED_SCHEMA
    elif topic == "weather-sun":
        out_topic = "weather-sun-enriched"
        hdfs_subdir = "sun"
        out_schema_str = SUN_ENRICHED_SCHEMA
    else:
        raise ValueError(f"Unsupported topic: {topic}")

    # Construct Full HDFS URI
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000")
    hdfs_namenode = hdfs_namenode.rstrip("/")
    hdfs_output_path = f"{hdfs_namenode}/raw/forecast/{hdfs_subdir}"

    # 3. Read Stream - Conservative Settings
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("BOOTSTRAP_SERVERS", "kafka-g5:9092"))
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "500")  # Keep conservative limit
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "30000")
        .load()
    )

    # 4. Deserialize & Enrich Logic
    # 🔧 CRITICAL FIX: Skip the first 5 bytes (Confluent Magic Byte + Schema ID)
    # Using expr("substring(value, 6)") converts the binary data to exclude the header
    # Also added mode=PERMISSIVE to prevent crashing on bad records
    parsed = df.select(
        from_avro(
            expr("substring(value, 6)"),
            input_schema_json,
            {"mode": "PERMISSIVE"}
        ).alias("data"),
        col("timestamp")
    )

    # Filter out nulls if deserialization failed due to PERMISSIVE mode
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
        .withColumn("DkArea", add_dk_area_udf(col("lon")))
        .withColumn("MunicipalityCode", add_municipality_code_udf(col("lat"), col("lon")))
    )

    # Prepare Serializer for this topic
    avro_serializer = AvroSerializer(schema_registry_client, out_schema_str)
    string_serializer = StringSerializer('utf_8')

    producer_conf = {'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS", "kafka-g5:9092")}

    # 5. Define foreachBatch Logic (Dual Sink: HDFS + Confluent Kafka)
    def write_to_kafka_and_hdfs(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        try:
            # --- Sink 1: HDFS (Spark Native Write) ---
            print(f"Writing batch {batch_id} to HDFS: {hdfs_output_path}")
            (batch_df.write
             .mode("append")
             .format("avro")
             .save(hdfs_output_path))

            # --- Sink 2: Kafka (Confluent Python Producer) ---
            rows = batch_df.collect()

            producer = SerializingProducer(producer_conf)

            for row in rows:
                try:
                    record = row.asDict()
                    # Ensure lat/lon are not None for the key
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
            print(f"Processed batch {batch_id} for {topic} (Sent {len(rows)} records to Kafka)")

        except Exception as e:
            print(f"Error in batch {batch_id} for {topic}: {e}")
            traceback.print_exc()

    # 6. Start Stream
    checkpoint_location = f"{checkpoint_root}/{topic.replace('/', '_')}_chkpt"

    query = (
        enriched_df.writeStream
        .foreachBatch(write_to_kafka_and_hdfs)
        .queryName(f"Enricher_{topic}")
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    print(f"Started stream: {topic} -> Kafka({out_topic}) & HDFS({hdfs_output_path})")
    return query


def main():
    global schema_registry_client

    # Load Envs
    checkpoint_root = os.getenv("CHECKPOINT_ROOT", "/tmp/spark/checkpoints/kafka_enricher")
    trigger_interval = os.getenv("TRIGGER_INTERVAL", "30 seconds")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    municipality_csv_path = os.getenv("MUNICIPALITY_CSV", "data/municipality_codes_to_coordinates.csv")
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

    spark.sparkContext.setLogLevel("WARN")

    # Init Lookup
    if os.path.exists(municipality_csv_path):
        init_municipality_lookup(spark, municipality_csv_path)
    else:
        print("WARNING: Municipality CSV not found, skipping enrichment init.")
        import enrichers
        enrichers.bc_municipality_coords = spark.sparkContext.broadcast(None)
        enrichers.bc_municipality_codes = spark.sparkContext.broadcast(None)

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