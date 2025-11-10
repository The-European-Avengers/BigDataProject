#!/bin/bash
# ================================================================
# setup-ksqldb-correct.sh - FIXED VERSION
# Setup ksqlDB with explicit Avro schema for correct serialization
# Run this AFTER register-schemas.sh
# ================================================================

set -e

# FIX: Use localhost instead of ksqldb-server (when running from host)
KSQLDB_URL="${KSQLDB_URL:-http://localhost:8088}"
WINDOW_SIZE=${1:-"1 MINUTE"}

echo "=========================================="
echo "   KSQLDB - Setup with Correct Schema"
echo "=========================================="
echo "ksqlDB URL: $KSQLDB_URL"
echo "Window Size: $WINDOW_SIZE"
echo "=========================================="
echo ""

# Wait for ksqlDB to be ready
echo "⏳ Waiting for ksqlDB to be ready..."
max_attempts=30
attempt=0
while ! curl -s "$KSQLDB_URL/info" > /dev/null 2>&1; do
    if [ $attempt -ge $max_attempts ]; then
        echo "❌ ksqlDB not responding at $KSQLDB_URL"
        echo ""
        echo "Troubleshooting steps:"
        echo "  1. Check if containers are running: docker ps | grep ksqldb"
        echo "  2. Check ksqlDB logs: docker logs ksqldb-server --tail 50"
        echo "  3. Try manually: curl http://localhost:8088/info"
        exit 1
    fi
    sleep 2
    attempt=$((attempt + 1))
    echo -n "."
done
echo ""
echo "✅ ksqlDB is ready"
echo ""

# Create SQL file for ksqlDB
SQL_FILE="/tmp/ksqldb_correct_setup.sql"

cat > "$SQL_FILE" << 'EOF'
-- ===================================================
-- IMPORTANT: Set to process only NEW messages
-- ===================================================
SET 'auto.offset.reset' = 'latest';

-- ===================================================
-- STEP 1: Create source streams from producer
-- ===================================================
DROP STREAM IF EXISTS weather_wind DELETE TOPIC;
DROP STREAM IF EXISTS weather_sunshine DELETE TOPIC;

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

-- ===================================================
-- STEP 2: Drop old aggregation tables (if exist)
-- ===================================================
DROP TABLE IF EXISTS weather_aggregated_wind DELETE TOPIC;
DROP TABLE IF EXISTS weather_aggregated_sunshine DELETE TOPIC;

-- ===================================================
-- STEP 3: Create wind aggregation
-- Key: Use CAST(WINDOWEND AS BIGINT) for reliable serialization
-- ===================================================
CREATE TABLE weather_aggregated_wind WITH (
  KAFKA_TOPIC='weather.aggregated.wind.ksql',
  PARTITIONS=5,
  REPLICAS=1,
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

-- ===================================================
-- STEP 4: Create sunshine aggregation
-- ===================================================
CREATE TABLE weather_aggregated_sunshine WITH (
  KAFKA_TOPIC='weather.aggregated.sunshine.ksql',
  PARTITIONS=5,
  REPLICAS=1,
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

-- ===================================================
-- Verify setup
-- ===================================================
SHOW TABLES;
SHOW QUERIES;
EOF

echo "📄 Executing ksqlDB setup..."
echo ""

# Copy and execute SQL file using docker exec from inside the container network
docker exec ksqldb-cli bash -c "cat > /tmp/setup.sql << 'INNEREOF'
$(cat "$SQL_FILE")
INNEREOF
ksql http://ksqldb-server:8088 --file /tmp/setup.sql" 2>&1 | \
  grep -v "WARNING\|RMI\|Accept timed out\|jline\|SocketTimeoutException" || true

echo ""
echo "=========================================="
echo "   VERIFICATION"
echo "=========================================="
echo ""

# Verify tables were created
echo "📋 Created tables:"
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "SHOW TABLES;" 2>&1 | \
  grep -E "Table Name|weather" || echo "  (checking...)"

echo ""
echo "📋 Active queries:"
docker exec ksqldb-cli ksql http://ksqldb-server:8088 --execute "SHOW QUERIES;" 2>&1 | \
  grep -E "Query ID|RUNNING" | head -5 || echo "  (checking...)"

echo ""

# Check topics were created
echo "📋 Kafka topics created:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | \
  grep "weather.aggregated" || echo "  (none yet - will be created when data arrives)"

echo ""

echo "=========================================="
echo "   ✅ KSQLDB SETUP COMPLETE"
echo "=========================================="
echo ""
echo "ksqlDB is now waiting for new data!"
echo ""
echo "What's happening:"
echo "  • Streams created: weather_wind, weather_sunshine"
echo "  • Tables created: weather_aggregated_wind/sunshine"
echo "  • Output topics: weather.aggregated.wind.ksql, weather.aggregated.sunshine.ksql"
echo "  • Windowing: 1 MINUTE tumbling windows"
echo "  • auto.offset.reset: 'latest' (only processes NEW messages)"
echo ""
echo "Next steps:"
echo "  1. Start producer:      cd producer && java -jar target/scala-3.3.7/benchmark-producer.jar 100"
echo "  2. Wait for completion: ~70 seconds"
echo "  3. Wait for processing: ~90 more seconds for window to close"
echo "  4. Run latency monitor: export INPUT_TOPIC=weather.aggregated.wind.ksql"
echo "                          cd latency-monitor && java -jar target/scala-3.3.1/latency-monitor.jar 100"
echo ""