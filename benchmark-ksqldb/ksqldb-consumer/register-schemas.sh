#!/bin/bash
# ================================================================
# register-schemas.sh
# Register Avro schemas with Confluent Schema Registry
# Run this BEFORE starting ksqlDB
# ================================================================

set -e

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"

echo "=========================================="
echo "   SCHEMA REGISTRY - Schema Registration"
echo "=========================================="
echo "Schema Registry URL: $SCHEMA_REGISTRY_URL"
echo "=========================================="
echo ""

# Wait for Schema Registry to be ready
echo "⏳ Waiting for Schema Registry..."
max_attempts=30
attempt=0
while ! curl -s "$SCHEMA_REGISTRY_URL/subjects" > /dev/null 2>&1; do
    if [ $attempt -ge $max_attempts ]; then
        echo "❌ Schema Registry not responding"
        exit 1
    fi
    sleep 2
    attempt=$((attempt + 1))
done
echo "✅ Schema Registry is ready"
echo ""

# Define the schema (must match ksqlDB output exactly)
SCHEMA='{
  "type": "record",
  "name": "WeatherAggregated",
  "namespace": "ua.playground.benchmark",
  "fields": [
    {"name": "window_start", "type": "string"},
    {"name": "window_end", "type": "string"},
    {"name": "metric", "type": "string"},
    {"name": "stationId", "type": "int"},
    {"name": "stationName", "type": "string"},
    {"name": "avg_value", "type": "double"},
    {"name": "min_value", "type": "double"},
    {"name": "max_value", "type": "double"},
    {"name": "message_count", "type": "long"},
    {"name": "min_producer_ts", "type": "long"},
    {"name": "processing_end_ts", "type": "long"}
  ]
}'

echo "📋 Schema to register:"
echo "$SCHEMA" | jq .
echo ""

# Register schema for wind topic
echo "📤 Registering weather.aggregated.wind.ksql-value..."
RESPONSE=$(curl -s -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": $(echo "$SCHEMA" | jq -c .)}" \
  "$SCHEMA_REGISTRY_URL/subjects/weather.aggregated.wind.ksql-value/versions")

SCHEMA_ID=$(echo "$RESPONSE" | jq -r '.id // empty')
if [ -z "$SCHEMA_ID" ]; then
    echo "⚠️  Response: $RESPONSE"
    echo "   (Schema may already exist - this is OK)"
else
    echo "✅ Registered with schema ID: $SCHEMA_ID"
fi
echo ""

# Register schema for sunshine topic
echo "📤 Registering weather.aggregated.sunshine.ksql-value..."
RESPONSE=$(curl -s -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\": $(echo "$SCHEMA" | jq -c .)}" \
  "$SCHEMA_REGISTRY_URL/subjects/weather.aggregated.sunshine.ksql-value/versions")

SCHEMA_ID=$(echo "$RESPONSE" | jq -r '.id // empty')
if [ -z "$SCHEMA_ID" ]; then
    echo "⚠️  Response: $RESPONSE"
    echo "   (Schema may already exist - this is OK)"
else
    echo "✅ Registered with schema ID: $SCHEMA_ID"
fi
echo ""

# Verify registration
echo "=========================================="
echo "   VERIFICATION"
echo "=========================================="
echo ""

echo "📋 Registered subjects containing 'weather':"
curl -s "$SCHEMA_REGISTRY_URL/subjects" | jq '.[] | select(contains("weather"))' || echo "  (None found)"
echo ""

echo "📋 Schema for weather.aggregated.wind.ksql-value (latest version):"
curl -s "$SCHEMA_REGISTRY_URL/subjects/weather.aggregated.wind.ksql-value/versions/latest" | jq . || echo "  (Not found)"
echo ""

echo "=========================================="
echo "   ✅ SCHEMA REGISTRATION COMPLETE"
echo "=========================================="
echo ""
echo "Next: Start ksqlDB with the registered schemas"
echo ""