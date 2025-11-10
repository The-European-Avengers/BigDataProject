#!/bin/bash

set -e

NAMESPACE="bd-bd-gr-05"
THROUGHPUT=${1:-100}
KAFKA_POD="kafka-g5-controller-0"

echo "=========================================="
echo "  SPARK STREAMING BENCHMARK (K8S)"
echo "=========================================="
echo "Throughput: $THROUGHPUT msg/s"
echo "Namespace: $NAMESPACE"
echo "Registry: GitLab SDU"
echo "=========================================="
echo ""

# Step 1: Deploy infrastructure
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
kubectl get pods -n $NAMESPACE

# Step 2: Create Kafka topics
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

# Step 3: Run producer
echo "🚀 Step 3: Starting Producer..."
kubectl delete job benchmark-producer -n $NAMESPACE 2>/dev/null || true
sleep 2
kubectl apply -f k8s/jobs.yaml

echo "✅ Producer job created"
echo ""
echo "📊 Monitoring producer..."
kubectl wait --for=condition=complete job/benchmark-producer -n $NAMESPACE --timeout=180s || true

echo ""
echo "⏳ Waiting 40s for Spark to process data..."
sleep 40

# Step 4: Run latency monitor
echo ""
echo "⏱️  Step 4: Running Latency Monitor..."
kubectl delete job latency-monitor -n $NAMESPACE 2>/dev/null || true
sleep 2
kubectl apply -f k8s/jobs.yaml

echo "✅ Latency Monitor job created"
echo ""
kubectl wait --for=condition=complete job/latency-monitor -n $NAMESPACE --timeout=120s || true

# Step 5: Display results
echo ""
echo "=========================================="
echo "  📊 BENCHMARK RESULTS"
echo "=========================================="
echo ""

echo "🔹 Producer Results:"
kubectl logs -n $NAMESPACE job/benchmark-producer --tail=30 2>/dev/null || echo "(No logs yet)"
echo ""

echo "🔹 Latency Monitor Results:"
kubectl logs -n $NAMESPACE job/latency-monitor --tail=60 2>/dev/null || echo "(No logs yet)"
echo ""

echo "🔹 Spark Consumer Logs:"
kubectl logs -n $NAMESPACE -l app=spark-consumer --tail=20 2>/dev/null || echo "(No logs yet)"
echo ""

echo "=========================================="
echo "  ✅ BENCHMARK COMPLETE"
echo "=========================================="
echo ""
echo "📊 View more details:"
echo "  Pods: kubectl get pods -n $NAMESPACE"
echo "  Logs: kubectl logs -n $NAMESPACE -l app=spark-consumer -f"
echo "  Spark UI: kubectl port-forward -n $NAMESPACE svc/spark-consumer 4040:4040"
