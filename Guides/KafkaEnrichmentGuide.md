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

## Running the Pipeline

### Step 1: Start Producers

Producers fetch data from the DMI API and send it to Kafka.

```bash
# Start all 3 producers (wind, temp, sun)
kubectl rollout restart deployment/kafka-producer-1-g5 -n bd-bd-gr-05
kubectl rollout restart deployment/kafka-producer-2-g5 -n bd-bd-gr-05
kubectl rollout restart deployment/kafka-producer-3-g5 -n bd-bd-gr-05

# Wait for producers to be ready
kubectl rollout status deployment/kafka-producer-1-g5 -n bd-bd-gr-05
kubectl rollout status deployment/kafka-producer-2-g5 -n bd-bd-gr-05
kubectl rollout status deployment/kafka-producer-3-g5 -n bd-bd-gr-05
```

### Step 2: Monitor Producers (Wait for Completion)

Watch the logs to confirm producers successfully sent data.

```bash
# Watch wind producer logs
kubectl logs -f deployment/kafka-producer-1-g5 -n bd-bd-gr-05

# Look for these success messages:
# ✓ "Successfully sent X records to Kafka topic 'weather-wind'"
# Example: "✓ Successfully sent 234567 records to Kafka topic 'weather-wind'"

# Press Ctrl+C when you see the success message
```

**Expected Timeline**: 
- API connection: ~5-10 seconds
- Data download: ~30-60 seconds (depends on API response time)
- Sending to Kafka: ~10-20 seconds
- **Total**: ~1-2 minutes per producer

### Step 3: Verify Schemas are Registered

Before starting the enricher, confirm that schemas were registered by the producers.

```bash
# Check Schema Registry for registered schemas
kubectl run test --rm -i --restart=Never --image=curlimages/curl -n bd-bd-gr-05 -- \
  curl http://schema-registry:8081/subjects

# Expected output:
# ["weather-wind-value","weather-temp-value","weather-sun-value"]

# If you see an empty list [], wait 30 seconds and try again
```

### Step 4: Start Enricher

Once schemas are registered, start the enricher.

```bash
# Restart enricher (this pulls latest image and restarts)
kubectl rollout restart deployment/kafka-enricher-artem -n bd-bd-gr-05

# Alternative: Delete pod to force restart
# kubectl delete pod -l app=kafka-enricher-artem -n bd-bd-gr-05
```

### Step 5: Monitor Enricher

Watch the enricher process data and write to Kafka + HDFS.

```bash
# Watch enricher logs
kubectl logs -f -l app=kafka-enricher-artem -n bd-bd-gr-05

# Look for these success messages:
# ✓ "Successfully fetched schema for weather-wind-value"
# ✓ "Started stream: weather-wind -> Kafka(weather-wind-enriched) & HDFS(...)"
# ✓ "Processed batch X for weather-wind (Sent 500 records to Kafka)"

# Press Ctrl+C when you see successful batch processing
```

**Expected Output**:
```
Creating stream for topic: weather-wind
Successfully fetched schema for weather-wind-value
Started stream: weather-wind -> Kafka(weather-wind-enriched) & HDFS(hdfs://namenode-g5:9000/raw/forecast/wind)

Creating stream for topic: weather-temp
Successfully fetched schema for weather-temp-value
Started stream: weather-temp -> Kafka(weather-temp-enriched) & HDFS(hdfs://namenode-g5:9000/raw/forecast/temp)

Creating stream for topic: weather-sun
Successfully fetched schema for weather-sun-value
Started stream: weather-sun -> Kafka(weather-sun-enriched) & HDFS(hdfs://namenode-g5:9000/raw/forecast/sun)

All streams started. Checkpoint dir: /tmp/spark/checkpoints/kafka_enricher
Writing batch 0 to HDFS: hdfs://namenode-g5:9000/raw/forecast/wind
Processed batch 0 for weather-wind (Sent 500 records to Kafka)
============================================================
STREAM PROGRESS: Enricher_weather-wind
Status: Waiting for next trigger
Input rows: 500
Batch ID: 0
============================================================
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

# Expected output:
# drwxr-xr-x   - sparkuser supergroup  0 2025-11-28 12:02 /raw/forecast/sun
# drwxr-xr-x   - sparkuser supergroup  0 2025-11-28 12:02 /raw/forecast/temp
# drwxr-xr-x   - sparkuser supergroup  0 2025-11-28 12:02 /raw/forecast/wind

# Count files in each directory
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -count /raw/forecast/wind

# Example output:
#            1           42             640895 /raw/forecast/wind
#        (dirs)      (files)           (bytes)

# List files
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /raw/forecast/wind | head -10
```

### Verify Data Schema (Enrichment Fields)

```bash
# View Avro schema to confirm DkArea and MunicipalityCode fields exist
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -cat /raw/forecast/wind/part-00000-*.avro | head -c 1000 | strings | grep -E "DkArea|MunicipalityCode"

# If you see "DkArea" and "MunicipalityCode" in output, enrichment is working! ✅
```

---

## Monitoring (Optional)

### Monitor with Redpanda Console (Visual UI)

```bash
# Port-forward Redpanda Console
kubectl port-forward -n bd-bd-gr-05 svc/redpanda-console 8080:8080

# Open browser: http://localhost:8080
# - View topics
# - Browse messages
# - Check consumer groups
```

### Monitor Producers

```bash
# Check producer status
kubectl get pods -n bd-bd-gr-05 | grep kafka-producer

# View logs for specific producer
kubectl logs -f deployment/kafka-producer-1-g5 -n bd-bd-gr-05
kubectl logs -f deployment/kafka-producer-2-g5 -n bd-bd-gr-05
kubectl logs -f deployment/kafka-producer-3-g5 -n bd-bd-gr-05
```

### Monitor Enricher

```bash
# Check enricher status
kubectl get pods -n bd-bd-gr-05 | grep kafka-enricher

# View enricher logs (real-time)
kubectl logs -f -l app=kafka-enricher-artem -n bd-bd-gr-05

# View last 100 lines
kubectl logs -l app=kafka-enricher-artem -n bd-bd-gr-05 --tail=100
```

### Monitor HDFS

```bash
# Check disk usage
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -du -h /raw/forecast

# Monitor file growth (updates every 10 seconds)
watch -n 10 'kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -count /raw/forecast/wind'
```

---

## Troubleshooting

### Problem 1: Enricher Shows 404 Error (Schema Not Found)

**Error Message**:
```
Failed to fetch schema for weather-wind-value: 404 Client Error
```

**Cause**: Producers haven't registered schemas yet.

**Solution**:
```bash
# Wait for producers to complete one cycle
kubectl logs -f deployment/kafka-producer-1-g5 -n bd-bd-gr-05
# Look for: "Successfully sent X records"

# Verify schemas exist
kubectl run test --rm -i --restart=Never --image=curlimages/curl -n bd-bd-gr-05 -- \
  curl http://schema-registry:8081/subjects

# If still empty, restart producers and wait again
```

### Problem 2: Producer Taking Too Long

**Symptoms**: Producer logs show "Fetching data..." but no progress.

**Solution**:
```bash
# Check producer logs for errors
kubectl logs deployment/kafka-producer-1-g5 -n bd-bd-gr-05 --tail=50

# Common causes:
# - DMI API slow response (normal, just wait)
# - Network issues (check pod describe)
# - API key invalid (check ConfigMap)

# If stuck for >5 minutes, restart:
kubectl rollout restart deployment/kafka-producer-1-g5 -n bd-bd-gr-05
```

### Problem 3: Enricher Crashing (CrashLoopBackOff)

**Symptoms**: `kubectl get pods` shows CrashLoopBackOff for enricher.

**Solution**:
```bash
# Check pod status
kubectl get pods -n bd-bd-gr-05 | grep kafka-enricher

# View logs from crashed pod
kubectl logs -l app=kafka-enricher-artem -n bd-bd-gr-05 --previous

# Common causes:
# 1. Schema Registry not reachable
# 2. Kafka not reachable
# 3. HDFS not reachable
# 4. Missing municipality CSV file

# Describe pod for more details
kubectl describe pod -l app=kafka-enricher-artem -n bd-bd-gr-05
```

### Problem 4: No Data in HDFS

**Symptoms**: `/raw/forecast` directories are empty.

**Solution**:
```bash
# Check if enricher is processing batches
kubectl logs -l app=kafka-enricher-artem -n bd-bd-gr-05 | grep "Processed batch"

# If no batches, check if Kafka topics have data
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- \
  kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic weather-wind --max-messages 5

# If topics are empty, restart producers
```

---

## Clean Restart (Fresh Start)

If you want to start completely fresh:

### Option 1: Quick Restart (Keep Producer Topics)

```bash
# Delete enriched topics (via Redpanda Console)
kubectl port-forward -n bd-bd-gr-05 svc/redpanda-console 8080:8080
# Open http://localhost:8080 → Topics → Delete weather-*-enriched topics

# Clean HDFS
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -rm -r -f /raw/forecast/wind
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -rm -r -f /raw/forecast/temp
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -rm -r -f /raw/forecast/sun

# Restart enricher
kubectl delete pod -l app=kafka-enricher-artem -n bd-bd-gr-05
kubectl logs -f -l app=kafka-enricher-artem -n bd-bd-gr-05
```

### Option 2: Complete Fresh Start (Delete Everything)

```bash
# 1. Delete all topics (via Redpanda Console)
kubectl port-forward -n bd-bd-gr-05 svc/redpanda-console 8080:8080
# Open http://localhost:8080 → Topics → Delete all weather-* topics

# 2. Clean HDFS
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -rm -r -f /raw/forecast/wind
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -rm -r -f /raw/forecast/temp
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -rm -r -f /raw/forecast/sun

# 3. Delete enricher pod
kubectl delete pod -l app=kafka-enricher-artem -n bd-bd-gr-05

# 4. Restart producers
kubectl rollout restart deployment/kafka-producer-1-g5 -n bd-bd-gr-05
kubectl rollout restart deployment/kafka-producer-2-g5 -n bd-bd-gr-05
kubectl rollout restart deployment/kafka-producer-3-g5 -n bd-bd-gr-05

# 5. Wait for producers (60 seconds)
sleep 60

# 6. Verify schemas
kubectl run test --rm -i --restart=Never --image=curlimages/curl -n bd-bd-gr-05 -- \
  curl http://schema-registry:8081/subjects

# 7. Restart enricher
kubectl rollout restart deployment/kafka-enricher-artem -n bd-bd-gr-05
kubectl logs -f -l app=kafka-enricher-artem -n bd-bd-gr-05
```

---



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

---

**Last Updated**: November 2025  
**Maintainer**: Artem Ziablov  
**Repository**: https://gitlab.sdu.dk/the-european-avengers/bigdataproject
