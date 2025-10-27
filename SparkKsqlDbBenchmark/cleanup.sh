#!/bin/bash
# ================================================================
# CLEANUP SCRIPT - Your Kafka Setup
# Save this as: cleanup.sh
# ================================================================

NAMESPACE="bd-bd-gr-05"
KAFKA_POD="kafka-g5-controller-0"

echo "=========================================="
echo "  BENCHMARK CLEANUP"
echo "=========================================="
echo "Namespace: $NAMESPACE"
echo "=========================================="
echo ""

echo "This will delete:"
echo "  • Spark Consumer deployment"
echo "  • Schema Registry deployment"
echo "  • Producer and Latency Monitor jobs"
echo "  • Weather Kafka topics"
echo ""
echo "⚠️  Your Kafka cluster (kafka-g5) will NOT be affected"
echo ""
read -p "Continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Cancelled."
    exit 0
fi

echo ""
echo "🗑️  Step 1: Deleting jobs..."
kubectl delete job benchmark-producer -n $NAMESPACE 2>/dev/null || echo "  (producer job not found)"
kubectl delete job latency-monitor -n $NAMESPACE 2>/dev/null || echo "  (latency monitor job not found)"
echo "✅ Jobs deleted"

echo ""
echo "🗑️  Step 2: Deleting deployments..."
kubectl delete deployment spark-consumer -n $NAMESPACE 2>/dev/null || echo "  (spark-consumer not found)"
kubectl delete deployment schema-registry -n $NAMESPACE 2>/dev/null || echo "  (schema-registry not found)"
echo "✅ Deployments deleted"

echo ""
echo "🗑️  Step 3: Deleting services..."
kubectl delete svc spark-consumer -n $NAMESPACE 2>/dev/null || echo "  (spark-consumer service not found)"
kubectl delete svc schema-registry -n $NAMESPACE 2>/dev/null || echo "  (schema-registry service not found)"
echo "✅ Services deleted"

echo ""
echo "🗑️  Step 4: Deleting Kafka topics..."
kubectl exec -n $NAMESPACE $KAFKA_POD -- \
    /opt/bitnami/kafka/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic weather.wind 2>/dev/null || echo "  (weather.wind not found)"
kubectl exec -n $NAMESPACE $KAFKA_POD -- \
    /opt/bitnami/kafka/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic weather.sunshine 2>/dev/null || echo "  (weather.sunshine not found)"
kubectl exec -n $NAMESPACE $KAFKA_POD -- \
    /opt/bitnami/kafka/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic weather.aggregated.output 2>/dev/null || echo "  (weather.aggregated.output not found)"
echo "✅ Topics deleted"

echo ""
echo "📋 Verifying cleanup..."
kubectl get pods -n $NAMESPACE | grep -E "(spark-consumer|schema-registry|benchmark-producer|latency-monitor)" || echo "  (no benchmark pods found - good!)"

echo ""
echo "✅ Cleanup complete!"
echo ""
echo "Your Kafka cluster (kafka-g5) is still running and unchanged."
echo ""