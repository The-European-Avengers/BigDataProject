#!/bin/bash

set -e

NAMESPACE="bd-bd-gr-05"
THROUGHPUT=${1:-100}
KAFKA_POD="kafka-g5-controller-0"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="./benchmark-results/${THROUGHPUT}msg-s_${TIMESTAMP}"

# Create results directory
mkdir -p "$RESULTS_DIR"

echo "=========================================="
echo "  SPARK STREAMING BENCHMARK (K8S)"
echo "=========================================="
echo "Throughput: $THROUGHPUT msg/s"
echo "Namespace: $NAMESPACE"
echo "Registry: GitLab SDU"
echo "Results: $RESULTS_DIR"
echo "=========================================="
echo ""

# ================================================================
# FUNCTION: Monitor Resources (Alternative to kubectl top)
# ================================================================
monitor_resources() {
    local duration=$1
    local output_file=$2
    
    echo "Timestamp,Pod,Status,Restarts,CPU_Request,CPU_Limit,Memory_Request,Memory_Limit,Age" > "$output_file"
    
    local start=$(date +%s)
    while [ $(($(date +%s) - start)) -lt $duration ]; do
        local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
        
        # Get pod status and resource info
        kubectl get pods -n $NAMESPACE -o custom-columns=\
NAME:.metadata.name,\
STATUS:.status.phase,\
RESTARTS:.status.containerStatuses[0].restartCount,\
AGE:.metadata.creationTimestamp \
--no-headers 2>/dev/null | while read name status restarts age; do
            
            # Get resource requests and limits
            local resources=$(kubectl get pod $name -n $NAMESPACE -o json 2>/dev/null | \
                jq -r '.spec.containers[0].resources | 
                       "cpu_req=\(.requests.cpu // "0"),cpu_lim=\(.limits.cpu // "0"),mem_req=\(.requests.memory // "0"),mem_lim=\(.limits.memory // "0")"')
            
            if [ -n "$resources" ]; then
                local cpu_req=$(echo $resources | grep -o 'cpu_req=[^,]*' | cut -d= -f2)
                local cpu_lim=$(echo $resources | grep -o 'cpu_lim=[^,]*' | cut -d= -f2)
                local mem_req=$(echo $resources | grep -o 'mem_req=[^,]*' | cut -d= -f2)
                local mem_lim=$(echo $resources | grep -o 'mem_lim=[^,]*' | cut -d= -f2)
                
                # Calculate pod age
                local pod_age=$(( ($(date +%s) - $(date -d "$age" +%s 2>/dev/null || date -j -f "%Y-%m-%dT%H:%M:%SZ" "$age" +%s)) / 60 ))
                
                echo "$timestamp,$name,$status,$restarts,$cpu_req,$cpu_lim,$mem_req,$mem_lim,${pod_age}m"
            fi
        done >> "$output_file"
        
        sleep 5
    done
}

# ================================================================
# FUNCTION: Collect Container Stats
# ================================================================
collect_container_stats() {
    local pod_name=$1
    local output_file=$2
    
    echo "Collecting container stats for $pod_name..."
    
    # Get detailed pod description
    kubectl describe pod $pod_name -n $NAMESPACE > "${output_file}.txt" 2>/dev/null
    
    # Get pod events
    kubectl get events -n $NAMESPACE --field-selector involvedObject.name=$pod_name \
        --sort-by='.lastTimestamp' > "${output_file}_events.txt" 2>/dev/null
}

# ================================================================
# Start background resource monitoring
# ================================================================
echo "📊 Starting resource monitoring..."
monitor_resources 300 "$RESULTS_DIR/resource-usage.csv" &
MONITOR_PID=$!

# Capture initial state
echo "📸 Capturing initial pod state..."
kubectl get pods -n $NAMESPACE -o wide > "$RESULTS_DIR/pods-initial.txt"
kubectl describe pods -n $NAMESPACE > "$RESULTS_DIR/pods-describe-initial.txt"

# ================================================================
# Step 1: Deploy infrastructure
# ================================================================
echo ""
echo "📦 Step 1: Deploying Schema Registry and Spark Consumer..."
kubectl apply -f k8s/deployment.yaml

echo "⏳ Waiting for Schema Registry..."
kubectl wait --for=condition=ready pod -l app=schema-registry -n $NAMESPACE --timeout=120s || true
sleep 20

echo "⏳ Waiting for Spark Consumer..."
kubectl wait --for=condition=ready pod -l app=spark-consumer -n $NAMESPACE --timeout=180s || true
sleep 30

# Check if pods are actually running
echo "📋 Checking pod status..."
kubectl get pods -n $NAMESPACE | tee "$RESULTS_DIR/pods-status.txt"

# ================================================================
# Step 2: Create Kafka topics
# ================================================================
echo ""
echo "📊 Step 2: Creating Kafka topics..."

create_topic() {
    local topic=$1
    echo "  Creating topic: $topic"
    kubectl exec -n $NAMESPACE $KAFKA_POD -- \
        /opt/bitnami/kafka/bin/kafka-topics.sh --create \
        --bootstrap-server localhost:9092 \
        --topic $topic \
        --partitions 5 \
        --replication-factor 3 \
        --if-not-exists 2>/dev/null || echo "  (topic may already exist)"
}

create_topic "weather.wind"
create_topic "weather.sunshine"
create_topic "weather.aggregated.output"

echo "✅ Topics created"
echo ""

# ================================================================
# Step 3: Run producer
# ================================================================
echo "🚀 Step 3: Starting Producer..."
PRODUCER_START=$(date +%s)

kubectl delete job benchmark-producer -n $NAMESPACE 2>/dev/null || true
sleep 2
kubectl apply -f k8s/jobs.yaml

echo "✅ Producer job created"
echo ""
echo "📊 Monitoring producer..."
kubectl wait --for=condition=complete job/benchmark-producer -n $NAMESPACE --timeout=180s || true

PRODUCER_END=$(date +%s)
PRODUCER_DURATION=$((PRODUCER_END - PRODUCER_START))

echo ""
echo "⏳ Waiting 40s for Spark to process data..."
sleep 40

# ================================================================
# Step 4: Run latency monitor
# ================================================================
echo ""
echo "⏱️  Step 4: Running Latency Monitor..."
MONITOR_START=$(date +%s)

kubectl delete job latency-monitor -n $NAMESPACE 2>/dev/null || true
sleep 2
kubectl apply -f k8s/jobs.yaml

echo "✅ Latency Monitor job created"
echo ""
kubectl wait --for=condition=complete job/latency-monitor -n $NAMESPACE --timeout=120s || true

MONITOR_END=$(date +%s)
MONITOR_DURATION=$((MONITOR_END - MONITOR_START))

# ================================================================
# Stop resource monitoring
# ================================================================
echo ""
echo "🛑 Stopping resource monitoring..."
kill $MONITOR_PID 2>/dev/null || true
wait $MONITOR_PID 2>/dev/null || true

# ================================================================
# Capture final state
# ================================================================
echo "📸 Capturing final pod state..."
kubectl get pods -n $NAMESPACE -o wide > "$RESULTS_DIR/pods-final.txt"
kubectl describe pods -n $NAMESPACE > "$RESULTS_DIR/pods-describe-final.txt"

# Collect detailed stats for key components
collect_container_stats "$(kubectl get pods -n $NAMESPACE -l app=spark-consumer -o jsonpath='{.items[0].metadata.name}')" \
    "$RESULTS_DIR/spark-consumer-stats"
collect_container_stats "$(kubectl get pods -n $NAMESPACE -l app=schema-registry -o jsonpath='{.items[0].metadata.name}')" \
    "$RESULTS_DIR/schema-registry-stats"

# ================================================================
# Step 5: Display results
# ================================================================
echo ""
echo "=========================================="
echo "  📊 BENCHMARK RESULTS"
echo "=========================================="
echo ""

echo "🔹 Producer Results:"
echo "----------------------------------------"
kubectl logs -n $NAMESPACE job/benchmark-producer --tail=30 2>/dev/null | tee "$RESULTS_DIR/producer-logs.txt" || echo "(No logs yet)"
echo ""

echo "🔹 Latency Monitor Results:"
echo "----------------------------------------"
kubectl logs -n $NAMESPACE job/latency-monitor --tail=60 2>/dev/null | tee "$RESULTS_DIR/latency-logs.txt" || echo "(No logs yet)"
echo ""

echo "🔹 Spark Consumer Logs:"
echo "----------------------------------------"
kubectl logs -n $NAMESPACE -l app=spark-consumer --tail=20 2>/dev/null | tee "$RESULTS_DIR/spark-consumer-logs.txt" || echo "(No logs yet)"
echo ""

# ================================================================
# Analyze resource usage
# ================================================================
echo "🔹 Resource Usage Analysis:"
echo "----------------------------------------"
if [ -f "$RESULTS_DIR/resource-usage.csv" ]; then
    echo ""
    echo "Top 5 Most Restarted Pods:"
    tail -n +2 "$RESULTS_DIR/resource-usage.csv" | \
        awk -F',' '{print $2 "," $4}' | \
        sort -t',' -k2 -rn | \
        head -5 | \
        column -t -s','
    
    echo ""
    echo "Pod Status Summary:"
    tail -n +2 "$RESULTS_DIR/resource-usage.csv" | \
        awk -F',' '{print $3}' | \
        sort | uniq -c | \
        awk '{printf "  %s: %d\n", $2, $1}'
    
    echo ""
    echo "Resource Allocation (Latest):"
    tail -20 "$RESULTS_DIR/resource-usage.csv" | \
        awk -F',' 'NR>1 {printf "  %-40s CPU: %s/%s  Memory: %s/%s\n", $2, $5, $6, $7, $8}' | \
        sort -u
else
    echo "  (Resource monitoring data not available)"
fi

# ================================================================
# Generate summary report
# ================================================================
TOTAL_DURATION=$(($(date +%s) - $(date -d "$(head -2 $RESULTS_DIR/resource-usage.csv | tail -1 | cut -d',' -f1)" +%s 2>/dev/null || echo 0)))

cat > "$RESULTS_DIR/SUMMARY.txt" << EOF
========================================
  BENCHMARK SUMMARY
========================================
Date: $(date)
Throughput: $THROUGHPUT msg/s
Namespace: $NAMESPACE

Duration:
  Producer: ${PRODUCER_DURATION}s
  Latency Monitor: ${MONITOR_DURATION}s
  Total: ${TOTAL_DURATION}s

Files Generated:
  - resource-usage.csv          : Resource monitoring data
  - producer-logs.txt           : Producer output
  - latency-logs.txt            : Latency analysis
  - spark-consumer-logs.txt     : Spark consumer logs
  - pods-initial.txt            : Initial pod state
  - pods-final.txt              : Final pod state
  - pods-describe-initial.txt   : Detailed initial state
  - pods-describe-final.txt     : Detailed final state
  - spark-consumer-stats.txt    : Spark consumer details
  - schema-registry-stats.txt   : Schema registry details

Location: $RESULTS_DIR
========================================
EOF

cat "$RESULTS_DIR/SUMMARY.txt"

echo ""
echo "=========================================="
echo "  ✅ BENCHMARK COMPLETE"
echo "=========================================="
echo ""
echo "📊 Results saved to: $RESULTS_DIR"
echo ""
echo "📋 View detailed results:"
echo "  Summary:        cat $RESULTS_DIR/SUMMARY.txt"
echo "  Resource usage: cat $RESULTS_DIR/resource-usage.csv"
echo "  Producer logs:  cat $RESULTS_DIR/producer-logs.txt"
echo "  Latency logs:   cat $RESULTS_DIR/latency-logs.txt"
echo ""
echo "🔍 Analyze resource usage:"
echo "  # View resource trends"
echo "  column -t -s',' $RESULTS_DIR/resource-usage.csv | less"
echo ""
echo "  # Extract Spark Consumer stats"
echo "  grep 'spark-consumer' $RESULTS_DIR/resource-usage.csv | column -t -s','"
echo ""
echo "📊 Quick stats:"
echo "  Pods: kubectl get pods -n $NAMESPACE"
echo "  Logs: kubectl logs -n $NAMESPACE -l app=spark-consumer -f"
echo "  Spark UI: kubectl port-forward -n $NAMESPACE svc/spark-consumer 4040:4040"
echo ""
echo "=========================================="