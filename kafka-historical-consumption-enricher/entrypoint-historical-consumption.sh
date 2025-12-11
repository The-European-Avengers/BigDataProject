#!/bin/bash
set -e

echo "================================================================================"
echo "HISTORICAL CONSUMPTION SPARK STREAMING CONSUMER - Starting"
echo "================================================================================"
echo "Spark Version: $(spark-submit --version 2>&1 | grep 'version' | head -1)"
echo "Python Version: $(python3 --version)"
echo "Current User: $(whoami)"
echo "Working Directory: $(pwd)"
echo "================================================================================"

# Configuration from environment variables
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-kafka-g5-controller-headless:9092}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
HDFS_NAMENODE="${HDFS_NAMENODE:-hdfs://namenode-g5:9000}"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-/tmp/spark/checkpoints/historical_consumption_enricher_v1}"
TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-1 day}"
MUNICIPALITY_CSV_HDFS="${MUNICIPALITY_CSV_HDFS:-hdfs://namenode-g5:9000/utils/municipality_codes_to_coordinates.csv}"

echo ""
echo "Configuration:"
echo "  BOOTSTRAP_SERVERS: $BOOTSTRAP_SERVERS"
echo "  SCHEMA_REGISTRY_URL: $SCHEMA_REGISTRY_URL"
echo "  HDFS_NAMENODE: $HDFS_NAMENODE"
echo "  CHECKPOINT_ROOT: $CHECKPOINT_ROOT"
echo "  TRIGGER_INTERVAL: $TRIGGER_INTERVAL"
echo "  MUNICIPALITY_CSV_HDFS: $MUNICIPALITY_CSV_HDFS"
echo ""
echo "================================================================================"
echo "Starting Spark Streaming Job..."
echo "================================================================================"

# Run Spark job with required packages
spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-avro_2.12:3.4.1,io.confluent:kafka-avro-serializer:7.4.0 \
  --repositories https://packages.confluent.io/maven/ \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:///opt/bitnami/spark/conf/log4j.properties" \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file:///opt/bitnami/spark/conf/log4j.properties" \
  --conf spark.sql.streaming.schemaInference=true \
  --conf spark.sql.adaptive.enabled=true \
  consumer-historical-consumption.py

echo ""
echo "================================================================================"
echo "Spark Streaming Job Ended"
echo "================================================================================"