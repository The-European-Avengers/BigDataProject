#!/bin/bash

RESULTS_DIR=${1:-"./benchmark-results/100msg-s_20251105_215250"}
CSV_FILE="$RESULTS_DIR/resource-usage.csv"

if [ ! -f "$CSV_FILE" ]; then
    echo "❌ Error: CSV file not found: $CSV_FILE"
    exit 1
fi

echo "=========================================="
echo "  📊 BENCHMARK RESOURCE ANALYSIS"
echo "=========================================="
echo "Results Directory: $RESULTS_DIR"
echo "Samples Collected: $(wc -l < "$CSV_FILE") rows"
echo ""

# ================================================================
# 1. Resource Allocation Summary
# ================================================================
echo "🔹 Resource Allocation per Component"
echo "----------------------------------------"
printf "%-30s | %-12s | %-11s | %-15s | %-14s\n" \
    "Component" "CPU Request" "CPU Limit" "Memory Request" "Memory Limit"
echo "-------------------------------|--------------|-------------|-----------------|---------------"

# Function to get latest resource values for a pod pattern
get_pod_resources() {
    local pattern=$1
    local name=$2
    grep "$pattern" "$CSV_FILE" | tail -1 | \
        awk -F',' -v name="$name" '{printf "%-30s | %-12s | %-11s | %-15s | %-14s\n", name, $5, $6, $7, $8}'
}

get_pod_resources "spark-consumer" "Spark Consumer"
get_pod_resources "schema-registry" "Schema Registry"
get_pod_resources "kafka-g5-controller-0" "Kafka Broker (each)"
get_pod_resources "benchmark-producer" "Producer (Job)"
get_pod_resources "latency-monitor" "Latency Monitor (Job)"

echo ""

# ================================================================
# 2. Pod Stability Analysis
# ================================================================
echo "🔹 Pod Stability During Test"
echo "----------------------------------------"

# Spark Consumer restart analysis
echo "Spark Consumer:"
INITIAL_RESTARTS=$(grep "spark-consumer" "$CSV_FILE" | head -1 | cut -d',' -f4)
FINAL_RESTARTS=$(grep "spark-consumer" "$CSV_FILE" | tail -1 | cut -d',' -f4)
RESTART_INCREASE=$((FINAL_RESTARTS - INITIAL_RESTARTS))
echo "  Initial restarts: $INITIAL_RESTARTS"
echo "  Final restarts: $FINAL_RESTARTS"
if [ "$RESTART_INCREASE" -gt 0 ]; then
    echo "  ⚠️  Restarted $RESTART_INCREASE time(s) during test"
else
    echo "  ✅ No restarts during test (stable)"
fi

# Check Kafka stability
echo ""
echo "Kafka Brokers:"
for i in 0 1 2; do
    POD="kafka-g5-controller-$i"
    INITIAL=$(grep "$POD" "$CSV_FILE" | head -1 | cut -d',' -f4)
    FINAL=$(grep "$POD" "$CSV_FILE" | tail -1 | cut -d',' -f4)
    INCREASE=$((FINAL - INITIAL))
    if [ "$INCREASE" -gt 0 ]; then
        echo "  ⚠️  Broker $i: Restarted $INCREASE time(s)"
    else
        echo "  ✅ Broker $i: Stable (0 restarts)"
    fi
done

echo ""

# ================================================================
# 3. Status Changes Timeline
# ================================================================
echo "🔹 Pod Status Changes"
echo "----------------------------------------"

# Extract status changes for key components
for component in "spark-consumer" "schema-registry" "benchmark-producer" "latency-monitor"; do
    STATUSES=$(grep "$component" "$CSV_FILE" | awk -F',' '{print $1 " - " $3}' | sort -u)
    if [ -n "$STATUSES" ]; then
        echo "$component:"
        echo "$STATUSES" | sed 's/^/  /'
        echo ""
    fi
done

# ================================================================
# 4. Resource Utilization Over Time
# ================================================================
echo "🔹 Test Timeline"
echo "----------------------------------------"
FIRST_TIMESTAMP=$(tail -n +2 "$CSV_FILE" | head -1 | cut -d',' -f1)
LAST_TIMESTAMP=$(tail -n +2 "$CSV_FILE" | tail -1 | cut -d',' -f1)
echo "Start: $FIRST_TIMESTAMP"
echo "End:   $LAST_TIMESTAMP"

# Calculate actual test duration
START_SEC=$(date -d "$FIRST_TIMESTAMP" +%s 2>/dev/null || date -j -f "%Y-%m-%d %H:%M:%S" "$FIRST_TIMESTAMP" +%s 2>/dev/null)
END_SEC=$(date -d "$LAST_TIMESTAMP" +%s 2>/dev/null || date -j -f "%Y-%m-%d %H:%M:%S" "$LAST_TIMESTAMP" +%s 2>/dev/null)
if [ -n "$START_SEC" ] && [ -n "$END_SEC" ]; then
    DURATION=$((END_SEC - START_SEC))
    echo "Duration: $DURATION seconds ($(echo "$DURATION / 60" | bc) minutes)"
fi

echo ""

# ================================================================
# 5. Component Summary
# ================================================================
echo "🔹 Component Activity Summary"
echo "----------------------------------------"

echo "Active pods during test:"
tail -n +2 "$CSV_FILE" | cut -d',' -f2 | sort -u | while read pod; do
    STATUS=$(grep "^[^,]*,$pod," "$CSV_FILE" | tail -1 | cut -d',' -f3)
    RESTARTS=$(grep "^[^,]*,$pod," "$CSV_FILE" | tail -1 | cut -d',' -f4)
    echo "  • $pod"
    echo "    Final status: $STATUS"
    echo "    Total restarts: $RESTARTS"
done

echo ""

# ================================================================
# 6. Resource Recommendations
# ================================================================
echo "🔹 Resource Analysis"
echo "----------------------------------------"

# Check if Spark Consumer was stable
if [ "$FINAL_RESTARTS" -gt 4 ]; then
    echo "⚠️  Spark Consumer restarted multiple times:"
    echo "   Consider increasing memory limits or investigating logs"
else
    echo "✅ Spark Consumer was stable with allocated resources"
fi

# Check Kafka stability
KAFKA_ISSUES=0
for i in 0 1 2; do
    POD="kafka-g5-controller-$i"
    RESTARTS=$(grep "$POD" "$CSV_FILE" | tail -1 | cut -d',' -f4)
    if [ "$RESTARTS" -gt 0 ]; then
        KAFKA_ISSUES=$((KAFKA_ISSUES + 1))
    fi
done

if [ "$KAFKA_ISSUES" -gt 0 ]; then
    echo "⚠️  $KAFKA_ISSUES Kafka broker(s) experienced restarts"
    echo "   Consider investigating Kafka logs and increasing resources"
else
    echo "✅ Kafka cluster was stable throughout the test"
fi

echo ""

# ================================================================
# 7. Performance Context
# ================================================================
echo "🔹 Performance Context"
echo "----------------------------------------"

# Extract latency from logs if available
if [ -f "$RESULTS_DIR/latency-logs.txt" ]; then
    AVG_LATENCY=$(grep "Average (Mean)" "$RESULTS_DIR/latency-logs.txt" | awk '{print $3, $4}')
    P99_LATENCY=$(grep "P99:" "$RESULTS_DIR/latency-logs.txt" | awk '{print $2, $3}')
    THROUGHPUT=$(grep "Total Throughput" "$RESULTS_DIR/producer-logs.txt" | awk '{print $3, $4}')
    
    echo "With the allocated resources:"
    echo "  • Average Latency: $AVG_LATENCY"
    echo "  • P99 Latency: $P99_LATENCY"
    echo "  • Throughput: $THROUGHPUT"
fi

echo ""
echo "=========================================="
echo "  ✅ ANALYSIS COMPLETE"
echo "=========================================="
echo ""
echo "💡 Tips:"
echo "  • View full data: column -t -s',' $CSV_FILE | less"
echo "  • Check Spark logs: cat $RESULTS_DIR/spark-consumer-logs.txt"
echo "  • Check events: cat $RESULTS_DIR/spark-consumer-stats_events.txt"
echo ""
