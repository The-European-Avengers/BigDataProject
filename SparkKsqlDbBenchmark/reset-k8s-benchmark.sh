#!/bin/bash

NAMESPACE="bd-bd-gr-05"
KAFKA_POD="kafka-g5-controller-0"

echo "=========================================="
echo "   K8S BENCHMARK RESET - COMPLETE CLEANUP"
echo "=========================================="
echo ""

# ================================================================
# Step 1: Delete Jobs
# ================================================================
echo "1️⃣  Deleting benchmark jobs..."
kubectl delete job benchmark-producer -n $NAMESPACE 2>/dev/null && echo "   ✅ Producer job deleted" || echo "   ℹ️  No producer job found"
kubectl delete job latency-monitor -n $NAMESPACE 2>/dev/null && echo "   ✅ Latency monitor job deleted" || echo "   ℹ️  No latency monitor job found"
echo ""

# ================================================================
# Step 2: Check Kafka Status
# ================================================================
echo "2️⃣  Checking Kafka status..."
KAFKA_READY=$(kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=kafka -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' | grep -c True)
KAFKA_TOTAL=$(kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=kafka --no-headers | wc -l)

if [ "$KAFKA_READY" -eq "$KAFKA_TOTAL" ] && [ "$KAFKA_TOTAL" -gt 0 ]; then
    echo "   ✅ Kafka cluster healthy ($KAFKA_READY/$KAFKA_TOTAL brokers ready)"
else
    echo "   ⚠️  WARNING: Kafka not fully ready ($KAFKA_READY/$KAFKA_TOTAL brokers)"
    echo "   Fix Kafka before running benchmark!"
    echo ""
    echo "   Check status: kubectl get pods -n $NAMESPACE | grep kafka"
    echo "   View logs: kubectl logs -n $NAMESPACE kafka-g5-controller-0"
fi
echo ""

# ================================================================
# Step 3: Delete Kafka Topics
# ================================================================
echo "3️⃣  Deleting Kafka topics..."

if [ "$KAFKA_READY" -gt 0 ]; then
    # List existing topics
    echo "   Current topics:"
    kubectl exec -n $NAMESPACE $KAFKA_POD -- \
        /opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null | \
        grep -E "weather\." | sed 's/^/     - /'
    
    echo ""
    echo "   Deleting weather topics..."
    
    for topic in "weather.wind" "weather.sunshine" "weather.aggregated.output"; do
        kubectl exec -n $NAMESPACE $KAFKA_POD -- \
            /opt/bitnami/kafka/bin/kafka-topics.sh --delete \
            --bootstrap-server localhost:9092 \
            --topic $topic 2>/dev/null && \
            echo "   ✅ Deleted: $topic" || \
            echo "   ℹ️  Topic not found: $topic"
    done
else
    echo "   ⚠️  Skipping topic deletion (Kafka not ready)"
fi
echo ""

# ================================================================
# Step 4: Clear Spark Checkpoints
# ================================================================
echo "4️⃣  Clearing Spark checkpoints..."
SPARK_POD=$(kubectl get pods -n $NAMESPACE -l app=spark-consumer -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [ -n "$SPARK_POD" ]; then
    kubectl exec -n $NAMESPACE $SPARK_POD -- rm -rf /tmp/spark/checkpoints/* 2>/dev/null && \
        echo "   ✅ Spark checkpoints cleared" || \
        echo "   ℹ️  No checkpoints to clear"
else
    echo "   ℹ️  Spark consumer not running"
fi
echo ""

# ================================================================
# Step 5: Restart Spark Consumer
# ================================================================
echo "5️⃣  Restarting Spark Consumer..."
kubectl delete pod -n $NAMESPACE -l app=spark-consumer 2>/dev/null && \
    echo "   ✅ Spark consumer restarted" || \
    echo "   ℹ️  Spark consumer not found"

# Wait for new pod to start
sleep 3
kubectl wait --for=condition=ready pod -l app=spark-consumer -n $NAMESPACE --timeout=60s 2>/dev/null && \
    echo "   ✅ New Spark consumer ready" || \
    echo "   ⚠️  Spark consumer taking longer to start"
echo ""

# ================================================================
# Step 6: Clean up old benchmark results (optional)
# ================================================================
echo "6️⃣  Cleaning up local results..."
if [ -d "./benchmark-results" ]; then
    echo "   Found $(find ./benchmark-results -type d -name '*msg-s*' | wc -l) result directories"
    read -p "   Delete old results? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf ./benchmark-results/*/
        echo "   ✅ Old results deleted"
    else
        echo "   ℹ️  Keeping old results"
    fi
else
    echo "   ℹ️  No local results to clean"
fi
echo ""

# ================================================================
# Step 7: Verify Clean State
# ================================================================
echo "7️⃣  Verifying clean state..."
echo ""
echo "   Pod Status:"
kubectl get pods -n $NAMESPACE | grep -E "spark-consumer|schema-registry|kafka" | \
    awk '{printf "     %-50s %s\n", $1, $3}'

echo ""
echo "   Job Status:"
JOBS=$(kubectl get jobs -n $NAMESPACE --no-headers 2>/dev/null | wc -l)
if [ "$JOBS" -eq 0 ]; then
    echo "     ✅ No active jobs"
else
    echo "     ⚠️  Found $JOBS active jobs:"
    kubectl get jobs -n $NAMESPACE --no-headers | awk '{printf "        - %s (%s)\n", $1, $2}'
fi

echo ""
echo "   Kafka Topics:"
if [ "$KAFKA_READY" -gt 0 ]; then
    WEATHER_TOPICS=$(kubectl exec -n $NAMESPACE $KAFKA_POD -- \
        /opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null | \
        grep -c "weather\." || echo "0")
    
    if [ "$WEATHER_TOPICS" -eq 0 ]; then
        echo "     ✅ No weather topics (clean slate)"
    else
        echo "     ⚠️  Found $WEATHER_TOPICS weather topics:"
        kubectl exec -n $NAMESPACE $KAFKA_POD -- \
            /opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null | \
            grep "weather\." | sed 's/^/        - /'
    fi
else
    echo "     ⚠️  Cannot verify (Kafka not ready)"
fi

echo ""
echo "=========================================="
echo "  ✅ RESET COMPLETE"
echo "=========================================="
echo ""
echo "Ready to run new benchmark:"
echo "  ./run-k8s-benchmark.sh 100"
echo ""
echo "=========================================="
