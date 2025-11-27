# consumer.py
import os
import threading
import time
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, lit
from pyspark.sql.types import StringType
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.streaming import Trigger

from enrichers import (
    init_municipality_lookup,
    add_dk_area_udf,
    add_municipality_code_udf
)
from schemas import WIND_ENRICHED_SCHEMA, TEMP_ENRICHED_SCHEMA, SUN_ENRICHED_SCHEMA

# Input topics
INPUT_TOPICS = ["weather-wind", "weather-temp", "weather-sun"]

def cleanup_checkpoint(path: str):
    import shutil, os
    try:
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"Cleaned old checkpoint: {path}")
    except Exception as e:
        print(f"Warning cleaning checkpoint: {e}")

def monitor_progress(query):
    """Daemon thread that prints streaming progress periodically."""
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
                    print("Batch duration (ms):", progress.get('batchDuration', 0))
                    print("=" * 60)
            except Exception:
                break
    threading.Thread(target=run, daemon=True).start()

def create_stream_for_topic(spark, topic: str, avro_schema_registry_url: str, checkpoint_root: str, trigger_interval="10 seconds"):
    """
    Create a streaming query that consumes `topic`, enriches records,
    and writes to the corresponding enriched topic.
    """
    print(f"Creating stream for topic: {topic}")

    df = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", os.getenv("BOOTSTRAP_SERVERS", "localhost:9092"))
             .option("subscribe", topic)
             .option("startingOffsets", "latest")
             .option("failOnDataLoss", "false")
             .load()
    )

    # Decode Avro using Schema Registry. Because spark.sql.avroSchemaRegistryUrl is set,
    # calling from_avro without schema will fetch writer schema from registry.
    parsed = df.select(from_avro(col("value")).alias("data"), col("timestamp"))

    # Extract columns (input Avro schema fields: lon, lat, value, step)
    flat = parsed.select(
        col("data.lon").alias("lon"),
        col("data.lat").alias("lat"),
        col("data.value").alias("value"),
        col("data.step").alias("step")
    )

    # Apply Pandas UDFs to produce DkArea and MunicipalityCode
    enriched = (
        flat
        .withColumn("DkArea", add_dk_area_udf(col("lon")))
        .withColumn("MunicipalityCode", add_municipality_code_udf(col("lat"), col("lon")))
    )

    # Choose output topic and schema according to input topic
    if topic == "weather-wind":
        out_topic = "wind_enriched"
        out_schema = WIND_ENRICHED_SCHEMA
    elif topic == "weather-temp":
        out_topic = "temp_enriched"
        out_schema = TEMP_ENRICHED_SCHEMA
    elif topic == "weather-sun":
        out_topic = "sun_enriched"
        out_schema = SUN_ENRICHED_SCHEMA
    else:
        raise ValueError(f"Unsupported topic: {topic}")

    # Build struct for Avro serialization
    payload = struct(
        col("lon").alias("lon"),
        col("lat").alias("lat"),
        col("value").alias("value"),
        col("step").alias("step"),
        col("DkArea").alias("DkArea"),
        col("MunicipalityCode").alias("MunicipalityCode")
    )

    kafka_out = (
        enriched
        .withColumn("key", (col("lat").cast(StringType()) + lit("_") + col("lon").cast(StringType())))
        .withColumn("value", to_avro(payload, out_schema))
        .select("key", "value")
    )

    checkpoint_location = f"{checkpoint_root}/{topic.replace('/', '_')}_chkpt"
    query = (
        kafka_out.writeStream
                 .format("kafka")
                 .outputMode("append")
                 .option("kafka.bootstrap.servers", os.getenv("BOOTSTRAP_SERVERS", "localhost:9092"))
                 .option("topic", out_topic)
                 .option("checkpointLocation", checkpoint_location)
                 .queryName(f"Enricher_{topic}")
                 .trigger(Trigger.ProcessingTime(trigger_interval))
                 .start()
    )

    print(f"Writing enriched data to: {out_topic} (checkpoint: {checkpoint_location})")
    return query

def main():
    checkpoint_root = os.getenv("CHECKPOINT_ROOT", "/tmp/spark/checkpoints/kafka_enricher")
    trigger_interval = os.getenv("TRIGGER_INTERVAL", "10 seconds")
    schema_registry = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    municipality_csv_path = os.getenv("MUNICIPALITY_CSV", "data/municipality_codes_to_coordinates.csv")

    cleanup_checkpoint(checkpoint_root)

    spark = (
        SparkSession.builder
        .appName("KafkaWeatherEnricher")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # Let Spark Avro functions use Schema Registry
        .config("spark.sql.avroSchemaRegistryUrl", schema_registry)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("Spark session created")

    # Initialise municipality lookup and broadcast variables
    init_municipality_lookup(spark, municipality_csv_path)
    print("Municipality lookup initialised and broadcasted")

    # Create one query per input topic
    queries = []
    try:
        for t in INPUT_TOPICS:
            q = create_stream_for_topic(spark, t, schema_registry, checkpoint_root, trigger_interval)
            monitor_progress(q)
            queries.append(q)

        # Await termination of all queries
        for q in queries:
            q.awaitTermination()

    except KeyboardInterrupt:
        print("Stopping due to keyboard interrupt")
    except Exception:
        print("Error in streaming job:")
        traceback.print_exc()
    finally:
        print("Stopping Spark session")
        spark.stop()

if __name__ == "__main__":
    main()
