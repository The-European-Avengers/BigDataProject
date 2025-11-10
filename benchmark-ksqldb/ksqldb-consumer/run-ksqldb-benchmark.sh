#!/bin/bash
# ================================================================
# run-ksqldb-benchmark.sh
# Clean workflow for ksqlDB benchmarking
# Usage: ./run-ksqldb-benchmark.sh [throughput]
# Example: ./run-ksqldb-benchmark.sh 100
# ================================================================

set -e

THROUGHPUT=${1:-100}

echo "=========================================="
echo "   KSQLDB STREAMING BENCHMARK"
echo "=========================================="
echo "Throughput: $THROUGHPUT msg/s"
echo "=========================================="
echo ""

# ================================================================
# STEP 0: Check ksqlDB server health
# ================================================================
echo "Step 0: Checking ksqlDB server health..."
SERVER_STATUS=$(docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "SHOW SERVER STATUS;" 2>&1 || true)

if [[ $SERVER_STATUS == *"DEGRADED"* ]] || [[ $SERVER_STATUS == *"UNAVAILABLE"* ]]; then
    echo "❌ ksqlDB server is not healthy (status: DEGRADED/UNAVAILABLE)."
    echo "   Please check logs and restore the command topic if needed."
    exit 1
fi

echo "✅ ksqlDB server is healthy"
echo ""

# ================================================================
# STEP 1: Cleanup old ksqlDB objects
# ================================================================
echo "Step 1: Cleaning up old queries..."
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "TERMINATE ALL;" 2>&1 | \
  grep -v "WARNING\|RMI\|Accept timed out\|jline" || true

sleep 3

echo "✅ Old queries terminated"
echo ""

# ================================================================
# STEP 2: Create fresh streams and tables
# ================================================================
echo "Step 2: Setting up ksqlDB streams and tables..."

# Set auto.offset.reset
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "SET 'auto.offset.reset' = 'earliest';"

# Drop old objects
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "DROP TABLE IF EXISTS weather_aggregated_wind DELETE TOPIC;"
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "DROP TABLE IF EXISTS weather_aggregated_sunshine DELETE TOPIC;"
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "DROP STREAM IF EXISTS weather_wind DELETE TOPIC;"
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "DROP STREAM IF EXISTS weather_sunshine DELETE TOPIC;"

docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "SET 'auto.offset.reset' = 'earliest';"

# Create source streams
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute \
"CREATE STREAM weather_wind (
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
);"

docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute \
"CREATE STREAM weather_sunshine (
  timeObserved VARCHAR,
  stationId INT KEY,
  stationName VARCHAR,
  metric VARCHAR,
  value DOUBLE,
  producer_ts BIGINT
) WITH (
  KAFKA_TOPIC='weather.sunshine',
  VALUE_FORMAT='AVRO',
  KEY_FORMAT='KAFKA',
  TIMESTAMP='producer_ts'
);"

# Create wind aggregation (streaming)
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute \
"CREATE TABLE weather_aggregated_wind WITH (
  KAFKA_TOPIC='weather.aggregated.wind.ksql',
  PARTITIONS=5,
  KEY_FORMAT='JSON',
  VALUE_FORMAT='AVRO'
) AS
SELECT
  stationId,
  stationName,
  TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_start,
  TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_end,
  metric,
  ROUND(AVG(value), 2) AS avg_value,
  ROUND(MIN(value), 2) AS min_value,
  ROUND(MAX(value), 2) AS max_value,
  COUNT(*) AS message_count,
  MIN(producer_ts) AS min_producer_ts,
  MAX(ROWTIME) AS processing_end_ts
FROM weather_wind
WINDOW TUMBLING (SIZE 1 MINUTES, GRACE PERIOD 1 SECOND)
GROUP BY stationId, stationName, metric
EMIT CHANGES;"

# Create sunshine aggregation (streaming)
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute \
"CREATE TABLE weather_aggregated_sunshine WITH (
  KAFKA_TOPIC='weather.aggregated.sunshine.ksql',
  PARTITIONS=5,
  KEY_FORMAT='JSON',
  VALUE_FORMAT='AVRO'
) AS
SELECT
  stationId,
  stationName,
  TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_start,
  TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_end,
  metric,
  ROUND(AVG(value), 2) AS avg_value,
  ROUND(MIN(value), 2) AS min_value,
  ROUND(MAX(value), 2) AS max_value,
  COUNT(*) AS message_count,
  MIN(producer_ts) AS min_producer_ts,
  CAST(WINDOWEND AS BIGINT) AS processing_end_ts
FROM weather_sunshine
WINDOW TUMBLING (SIZE 1 MINUTES, GRACE PERIOD 1 SECOND)
GROUP BY stationId, stationName, metric
EMIT CHANGES;"

# Fix cleanup.policy for ksqlDB-generated topic
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name weather.aggregated.sunshine.ksql \
  --add-config 'cleanup.policy=delete'

docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name weather.aggregated.sunshine.ksql \
  --add-config 'retention.ms=3600000'

# Optional: Show tables (non-blocking)
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "SHOW TABLES;"

echo ""
echo "✅ Streams and tables submitted (aggregation queries are running asynchronously)"
echo ""

echo "Deleting schema subjects..."
curl -X DELETE http://localhost:8081/subjects/weather.sunshine-value
curl -X DELETE http://localhost:8081/subjects/weather.wind-value
echo "✅ Schema subjects deleted"

# ================================================================
# STEP 3: Start Producer
# ================================================================
echo "Step 3: Starting producer..."
cd ../producer
java -jar target/scala-3.3.7/benchmark-producer.jar $THROUGHPUT
cd ..
echo ""
echo "✅ Producer completed"
echo ""


docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather.wind \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --timeout-ms 5000 \
  --max-messages 5

docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather.sunshine \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --timeout-ms 5000 \
  --max-messages 5

docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather.aggregated.wind.ksql \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --timeout-ms 5000 \
  --max-messages 5

docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather.aggregated.sunshine.ksql \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --timeout-ms 5000 \
  --max-messages 5

# ================================================================
# STEP 4: Wait for ksqlDB to process
# ================================================================
echo "Step 4: Waiting for ksqlDB processing..."
echo "   - Producer finished sending data"
echo "   - Waiting for windows to close and emit results (~90 seconds)"
echo ""

for i in {1..9}; do
    echo "   ⏳ Waiting... ($((i*10)) seconds elapsed)"
    sleep 10
done

echo ""
echo "✅ Processing complete"
echo ""

# ================================================================
# STEP 5: Run Latency Monitor
# ================================================================
# ================================================================
# Verify that output topic contains data before latency monitor
# ================================================================
echo "Step 5: Analyzing latency..."
echo ""

export INPUT_TOPIC="weather.aggregated.wind.ksql"
cd latency-monitor
java -jar target/scala-3.3.1/latency-monitor.jar $THROUGHPUT
cd ..

echo ""
echo "=========================================="
echo "   BENCHMARK COMPLETE"
echo "=========================================="
echo ""
echo "Results saved to: ./benchmark-results/"
echo ""