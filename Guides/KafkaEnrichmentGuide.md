# Quick Start Guide - Weather Data Enrichment Pipeline

This guide shows how to **run and restart** the weather data enrichment pipeline after the code has been deployed to the Kubernetes cluster.

## Prerequisites

✅ All infrastructure already deployed (Kafka, Schema Registry, HDFS)  
✅ Producer and Enricher Docker images already built and pushed to GitLab registry  
✅ Kubernetes deployments already applied  

---

## Architecture Quick Overview

```
DMI API → Producers → Kafka Topics → Enricher → Enriched Topics + HDFS
           (3 pods)   (weather-*)     (Spark)    (*-enriched)   (/raw/forecast)
```

**Data Flow**:
1. **Producers** fetch weather data (wind, temp, sun) from DMI API every 3 hours
2. **Kafka** stores raw data in topics: `weather-wind`, `weather-temp`, `weather-sun`
3. **Enricher** reads from Kafka, adds `DkArea` and `MunicipalityCode`, writes to:
   - Kafka enriched topics: `weather-wind-enriched`, `weather-temp-enriched`, `weather-sun-enriched`
   - HDFS: `/raw/forecast/wind`, `/raw/forecast/temp`, `/raw/forecast/sun`

---

## Main Commands and Running the Pipeline

## 0. Delete old HDFS data

```bash
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -rm -r -f /raw/forecast/wind
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -rm -r -f /raw/forecast/temp
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -rm -r -f /raw/forecast/sun
```

## 1. Restart producers

```bash
kubectl rollout restart deployment/kafka-producer-1-g5 -n bd-bd-gr-05
kubectl rollout restart deployment/kafka-producer-2-g5 -n bd-bd-gr-05
kubectl rollout restart deployment/kafka-producer-3-g5 -n bd-bd-gr-05
```

## 2. Watch one producer until completion

```bash
kubectl logs -f deployment/kafka-producer-1-g5 -n bd-bd-gr-05
```

Wait for log: "Successfully sent X records".

## 3. Verify Schema Registry

```bash
kubectl run test --rm -i --restart=Never --image=curlimages/curl -n bd-bd-gr-05 -- curl http://schema-registry:8081/subjects
```

## 4. Restart enricher

```bash
kubectl delete pod -l app=kafka-enricher-artem -n bd-bd-gr-05
```

## 5. Watch enricher logs

```bash
kubectl logs -f -l app=kafka-enricher-artem -n bd-bd-gr-05
```

---


## Verification - Confirm Pipeline is Working

### Verify Enriched Kafka Topics

```bash
# List all Kafka topics
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- \
  kafka-topics.sh --bootstrap-server localhost:9092 --list

# Should see both input and enriched topics:
# weather-wind
# weather-temp
# weather-sun
# weather-wind-enriched    ← Enricher output
# weather-temp-enriched    ← Enricher output
# weather-sun-enriched     ← Enricher output
```

### Verify HDFS Output

```bash
# Check HDFS directories exist
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /raw/forecast


# Count files in each directory
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -count /raw/forecast/wind
  (files)           (bytes)

# List files
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /raw/forecast/wind | head -10

```
---

## Important Notes

### Timing

- **Producers**: Run every 3 hours automatically (POLL_INTERVAL=10800 seconds)
- **Enricher**: Processes data every 30 seconds (TRIGGER_INTERVAL=30 seconds)
- **First run**: Takes ~2-3 minutes for full pipeline startup

### Resource Usage

```bash
# Check resource usage
kubectl top pod -n bd-bd-gr-05 | grep -E "producer|enricher"

# Typical usage:
# kafka-producer-1-g5:  CPU: 200m, Memory: 800Mi
# kafka-enricher-artem: CPU: 1000m, Memory: 2Gi
```

### Data Retention

- **Kafka topics**: Retention depends on Kafka configuration (default: 7 days)
- **HDFS**: Data persists until manually deleted
- **Checkpoints**: Stored in enricher pod's ephemeral storage

---

## Quick Reference

### Essential Commands

```bash
# Start pipeline
kubectl rollout restart deployment/kafka-producer-{1,2,3}-g5 -n bd-bd-gr-05
kubectl rollout restart deployment/kafka-enricher-artem -n bd-bd-gr-05

# Monitor
kubectl logs -f deployment/kafka-producer-1-g5 -n bd-bd-gr-05
kubectl logs -f -l app=kafka-enricher-artem -n bd-bd-gr-05

# Verify
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- kafka-topics.sh --bootstrap-server localhost:9092 --list
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -ls /raw/forecast

# Check schemas
kubectl run test --rm -i --restart=Never --image=curlimages/curl -n bd-bd-gr-05 -- curl http://schema-registry:8081/subjects

# Access Redpanda Console
kubectl port-forward -n bd-bd-gr-05 svc/redpanda-console 8080:8080
# http://localhost:8080
```


---

## Support

If you encounter issues:

1. **Check logs** first: `kubectl logs -f <pod-name> -n bd-bd-gr-05`
2. **Check pod status**: `kubectl describe pod <pod-name> -n bd-bd-gr-05`
3. **Verify connectivity**: Use `curl` test pods to check services
4. **Consult troubleshooting section** above

