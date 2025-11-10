#!/bin/bash
# reset-benchmark.sh
# Complete cleanup script for Kafka streaming benchmark

set -e  # Exit on any error

echo "========================================"
echo "   BENCHMARK RESET - COMPLETE CLEANUP"
echo "========================================"
echo ""

# ===========================================
# 1. Stop any running Java processes
# ===========================================
echo "1. Stopping running processes..."

if pgrep -f "benchmark-producer.jar" > /dev/null; then
    echo "   Stopping benchmark-producer..."
    pkill -f "benchmark-producer.jar" || true
    sleep 2
fi

if pgrep -f "spark-consumer.jar" > /dev/null; then
    echo "   Stopping spark-consumer..."
    pkill -f "spark-consumer.jar" || true
    sleep 2
fi

if pgrep -f "latency-monitor.jar" > /dev/null; then
    echo "   Stopping latency-monitor..."
    pkill -f "latency-monitor.jar" || true
    sleep 2
fi

# Check if any are still running
if pgrep -f "benchmark-producer.jar\|spark-consumer.jar\|latency-monitor.jar" > /dev/null; then
    echo "   ⚠️  Warning: Some processes still running, force killing..."
    pkill -9 -f "benchmark-producer.jar" || true
    pkill -9 -f "spark-consumer.jar" || true
    pkill -9 -f "latency-monitor.jar" || true
    sleep 2
fi

echo "   ✅ All processes stopped"
echo ""

# ===========================================
# 2. Check Docker/Kafka is running
# ===========================================
echo "2. Checking Kafka availability..."

if ! docker ps | grep -q kafka; then
    echo "   ⚠️  WARNING: Kafka container not running!"
    echo "   Please start Kafka first with: docker-compose up -d"
    exit 1
fi

echo "   ✅ Kafka is running"
echo ""

# ===========================================
# 3. Delete all Kafka topics
# ===========================================
echo "3. Deleting all Kafka topics..."

# Get all topics except internal ones
TOPICS=$(docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep -v "^__" | grep -v "Completed" | tr -d '\r' || true)

if [ -z "$TOPICS" ]; then
    echo "   ℹ️  No topics to delete"
else
    for topic in $TOPICS; do
        if [ ! -z "$topic" ]; then
            echo "   Deleting: $topic"
            docker exec kafka kafka-topics --delete \
                --bootstrap-server localhost:9092 \
                --topic "$topic" 2>/dev/null || true
        fi
    done
    echo "   ✅ Topics deleted"
fi

echo ""

# ===========================================
# 4. Clean Spark checkpoints
# ===========================================
echo "4. Cleaning Spark checkpoints..."

if [ -d "/tmp/spark/checkpoints" ]; then
    rm -rf /tmp/spark/checkpoints/*
    echo "   ✅ Checkpoints cleaned"
else
    echo "   ℹ️  No checkpoints directory found"
fi

echo ""

# ===========================================
# 5. Clean benchmark results (optional)
# ===========================================
echo "5. Cleaning old benchmark results..."

if [ -d "./benchmark-results" ]; then
    # Keep the directory but clean old reports
    find ./benchmark-results -name "*.txt" -type f -mtime +1 -delete 2>/dev/null || true
    echo "   ✅ Old results cleaned (kept recent files)"
else
    mkdir -p ./benchmark-results
    echo "   ✅ Created benchmark-results directory"
fi

echo ""

# ===========================================
# 6. Wait for Kafka to settle
# ===========================================
echo "6. Waiting for Kafka to settle..."
sleep 3
echo "   ✅ Kafka ready"
echo ""

# ===========================================
# 7. Recreate topics with proper configuration
# ===========================================
echo "7. Creating fresh Kafka topics..."

# Create weather.wind topic
if docker exec kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic weather.wind \
    --partitions 5 \
    --replication-factor 1 \
    --config retention.ms=3600000 2>/dev/null; then
    echo "   ✅ Created: weather.wind (5 partitions)"
else
    echo "   ⚠️  Topic weather.wind might already exist"
fi

# Create weather.sunshine topic
if docker exec kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic weather.sunshine \
    --partitions 5 \
    --replication-factor 1 \
    --config retention.ms=3600000 2>/dev/null; then
    echo "   ✅ Created: weather.sunshine (5 partitions)"
else
    echo "   ⚠️  Topic weather.sunshine might already exist"
fi

# Create weather.aggregated.output topic
if docker exec kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic weather.aggregated.output \
    --partitions 5 \
    --replication-factor 1 \
    --config retention.ms=3600000 2>/dev/null; then
    echo "   ✅ Created: weather.aggregated.output (5 partitions)"
else
    echo "   ⚠️  Topic weather.aggregated.output might already exist"
fi

echo ""

# ===========================================
# 8. Verify topics were created
# ===========================================
echo "8. Verifying topics..."
echo ""

ALL_TOPICS=$(docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep -v "^__" | tr -d '\r')

if echo "$ALL_TOPICS" | grep -q "weather.wind" && \
   echo "$ALL_TOPICS" | grep -q "weather.sunshine" && \
   echo "$ALL_TOPICS" | grep -q "weather.aggregated.output"; then
    echo "   ✅ All required topics exist:"
    echo "$ALL_TOPICS" | grep "weather\." | sed 's/^/      - /'
else
    echo "   ⚠️  WARNING: Not all topics were created successfully"
    echo "   Available topics:"
    echo "$ALL_TOPICS" | sed 's/^/      - /'
fi

echo ""

# ===========================================
# 9. Show topic details
# ===========================================
echo "9. Topic configuration:"
echo ""

docker exec kafka kafka-topics --describe \
    --bootstrap-server localhost:9092 \
    --topic weather.wind 2>/dev/null | grep -E "Topic:|Partition" | head -6

echo ""

# ===========================================
# Final Summary
# ===========================================
echo "========================================"
echo "   ✅ RESET COMPLETE!"
echo "========================================"
echo ""
echo "System is ready for benchmark testing:"
echo "  - All processes stopped"
echo "  - Topics recreated fresh:"
echo "    • weather.wind (5 partitions)"
echo "    • weather.sunshine (5 partitions)"
echo "    • weather.aggregated.output (5 partitions)"
echo "  - Spark checkpoints cleaned"
echo "  - Old benchmark results archived"
echo ""
echo "Next steps:"
echo "  1. Start Spark Consumer:      java -jar spark-consumer.jar 100"
echo "  2. Wait 20 seconds"
echo "  3. Start Producer:             java -jar benchmark-producer.jar 100"
echo "  4. Wait for completion + 20s"
echo "  5. Start Latency Monitor:      java -jar latency-monitor.jar 100"
echo ""
echo "========================================"