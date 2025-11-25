#!/bin/bash
NAMESPACE="bd-bd-gr-05"

echo "=== Testing Kafka Connectivity ==="

# Test 1: Can we resolve DNS?
echo "1. DNS Resolution Test..."
kubectl run dns-test --rm -i --restart=Never \
  --image=busybox --namespace=$NAMESPACE \
  -- nslookup kafka-g5-controller-0.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local

# Test 2: Can we reach Kafka broker?
echo "2. Kafka Connection Test..."
kubectl run kafka-check --rm -i --restart=Never \
  --image=bitnamilegacy/kafka:3.9.0-debian-12-r1 \
  --namespace=$NAMESPACE \
  -- kafka-broker-api-versions.sh \
  --bootstrap-server kafka-g5-controller-0.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092

# Test 3: Can producer image reach Kafka?
echo "3. Producer Image Network Test..."
kubectl run producer-net-test --rm -i --restart=Never \
  --image=registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/producer:latest \
  --namespace=$NAMESPACE \
  --env="KAFKA_BOOTSTRAP_SERVERS=kafka-g5-controller-0.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local:9092" \
  -- sh -c "getent hosts kafka-g5-controller-0.kafka-g5-controller-headless.bd-bd-gr-05.svc.cluster.local || echo 'DNS LOOKUP FAILED'"

echo "=== Tests Complete ==="
