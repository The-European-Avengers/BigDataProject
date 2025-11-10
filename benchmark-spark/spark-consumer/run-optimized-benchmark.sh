#!/bin/bash
# run-optimized-benchmark.sh
# Run Spark consumer with optimized settings

THROUGHPUT=${1:-100}
PROFILE=${2:-"low-latency"}  # Options: low-latency, high-throughput, balanced

echo "========================================"
echo "   OPTIMIZED SPARK BENCHMARK"
echo "========================================"
echo "Throughput: $THROUGHPUT msg/s"
echo "Profile: $PROFILE"
echo "========================================"
echo ""

# Set environment variables based on profile
case $PROFILE in
  "low-latency")
    echo "🚀 LOW LATENCY PROFILE"
    export WINDOW_DURATION="30 seconds"
    export TRIGGER_INTERVAL="1 second"
    export SHUFFLE_PARTITIONS="10"
    export MAX_OFFSETS_PER_TRIGGER="1000"
    export USE_EVENT_TIME="false"
    echo "   Window: 30 seconds"
    echo "   Trigger: 1 second"
    echo "   Expected latency: 2-5 seconds"
    ;;

  "high-throughput")
    echo "📊 HIGH THROUGHPUT PROFILE"
    export WINDOW_DURATION="5 minutes"
    export TRIGGER_INTERVAL="5 seconds"
    export SHUFFLE_PARTITIONS="20"
    export MAX_OFFSETS_PER_TRIGGER="10000"
    export USE_EVENT_TIME="false"
    echo "   Window: 5 minutes"
    echo "   Trigger: 5 seconds"
    echo "   Expected latency: 10-60 seconds"
    ;;

  "balanced")
    echo "⚖️  BALANCED PROFILE"
    export WINDOW_DURATION="1 minute"
    export TRIGGER_INTERVAL="2 seconds"
    export SHUFFLE_PARTITIONS="10"
    export MAX_OFFSETS_PER_TRIGGER="5000"
    export USE_EVENT_TIME="false"
    echo "   Window: 1 minute"
    echo "   Trigger: 2 seconds"
    echo "   Expected latency: 5-10 seconds"
    ;;

  *)
    echo "❌ Unknown profile: $PROFILE"
    echo "Available profiles: low-latency, high-throughput, balanced"
    exit 1
    ;;
esac

echo ""
echo "Starting Spark Consumer..."
echo "========================================"
echo ""

# Run Spark consumer with optimized settings
java -Xmx4g -Xms2g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=20 \
  -XX:InitiatingHeapOccupancyPercent=35 \
  -jar target/scala-3.3.7/spark-consumer.jar $THROUGHPUT