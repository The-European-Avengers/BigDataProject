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

docker exec ksqldb-cli ksql http://ksqldb-server:8088 << 'KSQL'
SET 'auto.offset.reset' = 'latest';

-- Drop old objects
DROP TABLE IF EXISTS weather_aggregated_wind DELETE TOPIC;
DROP TABLE IF EXISTS weather_aggregated_sunshine DELETE TOPIC;
DROP STREAM IF EXISTS weather_wind DELETE TOPIC;
DROP STREAM IF EXISTS weather_sunshine DELETE TOPIC;

-- Create source streams
CREATE STREAM weather_wind (
  timeObserved VARCHAR,
  stationId INT,
  stationName VARCHAR,
  metric VARCHAR,
  value DOUBLE,
  producer_ts BIGINT
) WITH (
  KAFKA_TOPIC='weather.wind',
  VALUE_FORMAT='AVRO'
);

CREATE STREAM weather_sunshine (
  timeObserved VARCHAR,
  stationId INT,
  stationName VARCHAR,
  metric VARCHAR,
  value DOUBLE,
  producer_ts BIGINT
) WITH (
  KAFKA_TOPIC='weather.sunshine',
  VALUE_FORMAT='AVRO'
);

-- Create wind aggregation
CREATE TABLE weather_aggregated_wind WITH (
  KAFKA_TOPIC='weather.aggregated.wind.ksql',
  PARTITIONS=5,
  KEY_FORMAT='JSON',
  VALUE_FORMAT='AVRO'
) AS
SELECT
  stationId AS stationId,
  LATEST_BY_OFFSET(stationName) AS stationName,
  TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_start,
  TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_end,
  metric AS metric,
  ROUND(AVG(value), 2) AS avg_value,
  ROUND(MIN(value), 2) AS min_value,
  ROUND(MAX(value), 2) AS max_value,
  COUNT(*) AS message_count,
  MIN(producer_ts) AS min_producer_ts,
  CAST(WINDOWEND AS BIGINT) AS processing_end_ts
FROM weather_wind 
WINDOW TUMBLING (SIZE 1 MINUTES)
GROUP BY stationId, metric
EMIT CHANGES;

-- Create sunshine aggregation
CREATE TABLE weather_aggregated_sunshine WITH (
  KAFKA_TOPIC='weather.aggregated.sunshine.ksql',
  PARTITIONS=5,
  KEY_FORMAT='JSON',
  VALUE_FORMAT='AVRO'
) AS
SELECT
  stationId AS stationId,
  LATEST_BY_OFFSET(stationName) AS stationName,
  TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_start,
  TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_end,
  metric AS metric,
  ROUND(AVG(value), 2) AS avg_value,
  ROUND(MIN(value), 2) AS min_value,
  ROUND(MAX(value), 2) AS max_value,
  COUNT(*) AS message_count,
  MIN(producer_ts) AS min_producer_ts,
  CAST(WINDOWEND AS BIGINT) AS processing_end_ts
FROM weather_sunshine 
WINDOW TUMBLING (SIZE 1 MINUTES)
GROUP BY stationId, metric
EMIT CHANGES;

SHOW TABLES;
KSQL

echo ""
echo "✅ Streams and tables created"
echo ""

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

# ================================================================
# STEP 4: Wait for ksqlDB to process
# ================================================================
echo "Step 4: Waiting for ksqlDB processing..."
echo "   - Producer finished sending data"
echo "   - Waiting for windows to close and emit results"
echo "   - This takes ~90 seconds for 1-minute windows"
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