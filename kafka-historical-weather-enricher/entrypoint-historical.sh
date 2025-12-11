#!/usr/bin/env bash
set -euo pipefail

echo "=============================================="
echo "Historical Weather Data Consumer Starting"
echo "=============================================="

# Default values can be overridden with environment variables at container runtime
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-kafka-g5:9092}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-/tmp/spark/checkpoints/historical_enricher}"
TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-60 seconds}"
HDFS_NAMENODE="${HDFS_NAMENODE:-hdfs://namenode-g5:9000}"
MUNICIPALITY_CSV_HDFS="${MUNICIPALITY_CSV_HDFS:-hdfs://namenode-g5:9000/utils/municipality_codes_to_coordinates.csv}"

# Spark packages required for Kafka + Avro + Schema Registry support
SPARK_PACKAGES="${SPARK_PACKAGES:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-avro_2.12:3.4.1,io.confluent:kafka-avro-serializer:7.4.0}"

echo "Configuration:"
echo "  Bootstrap Servers: ${BOOTSTRAP_SERVERS}"
echo "  Schema Registry: ${SCHEMA_REGISTRY_URL}"
echo "  HDFS Namenode: ${HDFS_NAMENODE}"
echo "  Checkpoint Root: ${CHECKPOINT_ROOT}"
echo "  Trigger Interval: ${TRIGGER_INTERVAL}"
echo "  Municipality CSV: ${MUNICIPALITY_CSV_HDFS}"
echo "=============================================="

# Wait for Kafka to be ready
echo ""
echo "⏳ Waiting for Kafka to be ready..."
KAFKA_HOST="${BOOTSTRAP_SERVERS%%:*}"
KAFKA_PORT="${BOOTSTRAP_SERVERS##*:}"
until timeout 5 bash -c "cat < /dev/null > /dev/tcp/${KAFKA_HOST}/${KAFKA_PORT}" 2>/dev/null; do
  echo "  Kafka not ready at ${KAFKA_HOST}:${KAFKA_PORT}, retrying in 5s..."
  sleep 5
done
echo "✓ Kafka is ready"

# Wait for Schema Registry to be ready
echo ""
echo "⏳ Waiting for Schema Registry to be ready..."
until curl -sf "${SCHEMA_REGISTRY_URL}/subjects" > /dev/null 2>&1; do
  echo "  Schema Registry not ready, retrying in 5s..."
  sleep 5
done
echo "✓ Schema Registry is ready"

# Wait for HDFS Namenode to be ready
echo ""
echo "⏳ Waiting for HDFS to be ready..."
HDFS_HOST=$(echo "${HDFS_NAMENODE}" | sed 's|hdfs://||' | cut -d':' -f1)
HDFS_PORT=$(echo "${HDFS_NAMENODE}" | sed 's|hdfs://||' | cut -d':' -f2)
until timeout 5 bash -c "cat < /dev/null > /dev/tcp/${HDFS_HOST}/${HDFS_PORT}" 2>/dev/null; do
  echo "  HDFS not ready at ${HDFS_HOST}:${HDFS_PORT}, retrying in 5s..."
  sleep 5
done
echo "✓ HDFS is ready"

echo ""
echo "=============================================="
echo "🚀 Starting Spark Streaming Consumer"
echo "=============================================="
echo ""

# Run spark-submit with the required packages for Kafka + Avro + Schema Registry support
# --repositories: Add Confluent Maven repository for kafka-avro-serializer
# --packages: Include Kafka, Avro, and Schema Registry dependencies
# --conf: Pass Schema Registry URL and HDFS configuration to Spark
exec spark-submit \
  --repositories https://packages.confluent.io/maven/ \
  --packages "${SPARK_PACKAGES}" \
  --conf "spark.sql.avroSchemaRegistryUrl=${SCHEMA_REGISTRY_URL}" \
  --conf "spark.hadoop.fs.defaultFS=${HDFS_NAMENODE}" \
  /home/sparkuser/consumer-historical.py