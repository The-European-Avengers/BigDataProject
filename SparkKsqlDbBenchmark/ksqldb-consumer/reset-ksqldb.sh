#!/bin/bash
# ================================================================
# reset-ksqldb.sh
# Hard reset ksqlDB to fix corruption
# ================================================================

set -e

echo "=========================================="
echo "   KSQLDB HARD RESET"
echo "=========================================="
echo ""

echo "Step 1: Stopping ksqlDB server..."
docker-compose stop ksqldb-server ksqldb-cli
sleep 5
echo "✅ Stopped"
echo ""

echo "Step 2: Deleting ksqlDB data volumes..."
# This removes the internal ksqlDB state
docker volume rm ksqldb_ksqldb_data 2>/dev/null || echo "   (Volume doesn't exist, that's OK)"
echo "✅ Data cleared"
echo ""

echo "Step 3: Deleting ksqlDB-related Kafka topics..."
# Delete the ksqlDB command topic and internal topics
docker exec kafka kafka-topics --delete \
  --bootstrap-server localhost:9092 \
  --topic "_confluent-ksql-default__command_topic" 2>/dev/null || echo "   (Command topic doesn't exist)"

docker exec kafka kafka-topics --delete \
  --bootstrap-server localhost:9092 \
  --topic "_confluent-ksql-default_query_CTAS_WEATHER_AGGREGATED_WIND_5-Aggregate-Aggregate-Materialize-changelog" 2>/dev/null || echo "   (Changelog doesn't exist)"

docker exec kafka kafka-topics --delete \
  --bootstrap-server localhost:9092 \
  --topic "_confluent-ksql-default_query_CTAS_WEATHER_AGGREGATED_SUNSHINE_7-Aggregate-Aggregate-Materialize-changelog" 2>/dev/null || echo "   (Changelog doesn't exist)"

docker exec kafka kafka-topics --delete \
  --bootstrap-server localhost:9092 \
  --topic "default_ksql_processing_log" 2>/dev/null || echo "   (Processing log doesn't exist)"

echo "✅ Internal topics deleted"
echo ""

echo "Step 4: Deleting output topics..."
docker exec kafka kafka-topics --delete \
  --bootstrap-server localhost:9092 \
  --topic "weather.aggregated.wind.ksql" 2>/dev/null || echo "   (Topic doesn't exist)"

docker exec kafka kafka-topics --delete \
  --bootstrap-server localhost:9092 \
  --topic "weather.aggregated.sunshine.ksql" 2>/dev/null || echo "   (Topic doesn't exist)"

echo "✅ Output topics deleted"
echo ""

echo "Step 5: Starting ksqlDB fresh..."
docker-compose up -d ksqldb-server ksqldb-cli
sleep 30
echo "✅ Started"
echo ""

echo "Step 6: Verifying ksqlDB is healthy..."
if curl -s http://localhost:8088/info | grep -q "RUNNING"; then
    echo "✅ ksqlDB is RUNNING"
elif curl -s http://localhost:8088/info | grep -q "DEGRADED"; then
    echo "⚠️  ksqlDB still DEGRADED, waiting..."
    sleep 30
fi
echo ""

echo "=========================================="
echo "   ✅ RESET COMPLETE"
echo "=========================================="
echo ""
echo "ksqlDB is now fresh and ready!"
echo ""
echo "Next steps:"
echo "  1. Reset Kafka benchmark topics:"
echo "     ./reset-benchmark.sh"
echo ""
echo "  2. Run ksqlDB benchmark:"
echo "     ./run-ksqldb-benchmark.sh 100"
echo ""