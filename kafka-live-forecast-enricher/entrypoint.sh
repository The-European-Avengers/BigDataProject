#!/usr/bin/env bash
set -euo pipefail

# Default values can be overridden with environment variables at container runtime
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-kafka-g5:9092}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
INPUT_TOPIC="${INPUT_TOPIC:-weather}"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-/tmp/spark_chkpt}"
TRIGGER="${TRIGGER:-10 seconds}"
# ADDED: org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 is required for .format("kafka")
SPARK_PACKAGES="${SPARK_PACKAGES:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-avro_2.12:3.4.1,io.confluent:kafka-avro-serializer:7.4.0}"

# Run spark-submit with the required packages for Avro + Schema Registry support
# ADDED: --repositories https://packages.confluent.io/maven/ so Spark can find the Confluent jars
exec spark-submit \
  --repositories https://packages.confluent.io/maven/ \
  --conf "spark.sql.avroSchemaRegistryUrl=${SCHEMA_REGISTRY_URL}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --packages "${SPARK_PACKAGES}" \
  /home/sparkuser/consumer.py \
  "${CHECKPOINT_ROOT}" "${TRIGGER}"