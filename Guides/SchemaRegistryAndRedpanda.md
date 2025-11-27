# Quick Start: Redpanda Console & Schema Registry

View your Kafka topics and schemas in 3 steps.

---

## Prerequisites

- `kubectl` installed
- Access to the kubeconfig file

---

## Setup (One-time)

```bash
# Verify connection
kubectl get pods -n bd-bd-gr-05
```
---
If no connection, set up kubeconfig file from your email, or check VPN connection if already have set up the cluster before.

## Access Redpanda Console

```bash
# Port forward (keep this running)
kubectl port-forward -n bd-bd-gr-05 svc/redpanda-console 8080:8080
```

**Open in browser:** http://localhost:8080

### What You'll See:
- **Topics**: All Kafka topics (weather-wind, weather-temp, weather-sun)
- **Messages**: Live data from topics
- **Schema Registry**: Registered Avro schemas
- **Brokers**: Kafka cluster health

---

## Access Schema Registry API (Optional)

**New terminal:**

```bash
export KUBECONFIG="$(pwd)/kubernetes/bd-gr-05-sa-bd-bd-gr-05-kubeconfig.yaml"
kubectl port-forward -n bd-bd-gr-05 svc/schema-registry 8081:8081
```

**Test API:**

```bash
# List schemas
curl http://localhost:8081/subjects

# Get schema details
curl http://localhost:8081/subjects/weather-wind-value/versions/latest | jq .
```

---

## View Live Messages

### Option 1: Redpanda Console (Easy)

1. Go to http://localhost:8080/topics
2. Click a topic → **Messages** tab
3. Click **Fetch Messages**

### Option 2: Command Line

```bash
KAFKA_POD=$(kubectl get pod -n bd-bd-gr-05 -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}')

kubectl exec -it $KAFKA_POD -n bd-bd-gr-05 -- \
  /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic weather-wind \
  --from-beginning \
  --max-messages 10
```

---

## Quick Reference

```bash
# Redpanda Console
kubectl port-forward -n bd-bd-gr-05 svc/redpanda-console 8080:8080
# → http://localhost:8080

# Schema Registry
kubectl port-forward -n bd-bd-gr-05 svc/schema-registry 8081:8081
# → http://localhost:8081/subjects

# Check status
kubectl get pods -n bd-bd-gr-05

# View logs
kubectl logs -n bd-bd-gr-05 <pod-name> --tail=50
```

---

## Troubleshooting

**Port forward fails?**
```bash
kubectl get svc -n bd-bd-gr-05
kubectl get pods -n bd-bd-gr-05
```

**No data in topics?**
```bash
kubectl logs -n bd-bd-gr-05 -l app=kafka-producer-1-g5 --tail=20
```

**Schema Registry empty?**
```bash
curl http://localhost:8081/subjects
```
