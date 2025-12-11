# Historical Weather Enrichment Pipeline - Complete Guide

## Table of Contents

1. [Quick Start](#quick-start)
2. [Architecture Overview](#architecture-overview)
3. [Key Features](#key-features)
4. [Deployment](#deployment)
5. [Monitoring](#monitoring)
6. [Verification](#verification)
7. [Quick Reference](#quick-reference)
8. [Troubleshooting](#troubleshooting)
9. [Notes](#notes)

---

## Quick Start

### Prerequisites
- Kafka cluster running (`kafka-g5-controller-headless:9092`)
- Schema Registry deployed (`schema-registry:8081`)
- HDFS namenode accessible (`namenode-g5:9000`)
- Municipality CSV in HDFS (`/utils/municipality_codes_to_coordinates.csv`)
- Historical weather topics populated with data

### Start Pipeline

```bash
# 1. Verify historical data topics exist
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- \
  kafka-topics.sh --bootstrap-server localhost:9092 --list | grep historical

# Expected output:
# historical-weather-wind
# historical-weather-temp
# historical-weather-sun

# 2. Deploy/restart historical enricher
kubectl apply -f kafka-historical-enricher.yaml
# OR
kubectl rollout restart deployment/kafka-historical-weather-enricher -n bd-bd-gr-05

# 3. Monitor enricher logs
kubectl logs -f deployment/kafka-historical-weather-enricher -n bd-bd-gr-05
# Look for: "BATCH X COMPLETED"
```

---

## Architecture Overview

```
┌─────────────────────────┐
│   Historical Weather    │  Historical data from DMI
│   Data Producer         │  Backfill: 2024-01-01 to present
│   + batchId tracking    │  Batch identifier per upload
└──────────┬──────────────┘
           │ Kafka Topics (Historical Raw)
           │ • historical-weather-wind
           │ • historical-weather-temp  
           │ • historical-weather-sun
           ▼
┌────────────────────────────────────────────┐
│  kafka-historical-weather-enricher         │
│  (Spark Streaming)                         │
│  ┌──────────────────────────────────────┐  │
│  │ Municipality Lookup                  │  │
│  │ /utils/municipality_...              │  │  311 Danish municipalities
│  │ Nearest-neighbor mapping             │  │  Euclidean distance
│  └──────────────────────────────────────┘  │
│                                            │
│  Enrichments:                              │
│  + dkArea (1 or 2)                         │  Longitude-based region
│  + municipalityCode (101-851)              │  Nearest municipality
│  + batchId (preserved)                     │  Upload batch tracking
│                                            │
│  Deduplication:                            │
│  • Key: (stationId, timeObserved)          │
│  • Strategy: Latest batchId wins           │
│  • Merge with existing monthly files      │
└──────┬─────────────────────────────────────┘
       │
       │ Monthly Avro Files (Overwrite mode)
       ▼
┌────────────────────────────────────────────┐
│           HDFS Storage                     │
│                                            │
│  /historical/YYYY/weather-{type}/MM.avro   │
│                                            │
│  Structure:                                │
│  ├─ 2024/                                  │
│  │  ├─ weather-wind/                       │
│  │  │  ├─ 01.avro  (Jan 2024 wind data)    │
│  │  │  ├─ 02.avro  (Feb 2024 wind data)    │
│  │  │  └─ ...                              │
│  │  ├─ weather-temp/                       │
│  │  │  ├─ 01.avro  (Jan 2024 temp data)    │
│  │  │  └─ ...                              │
│  │  └─ weather-sun/                        │
│  │     └─ ...                              │
│  └─ 2025/                                  │
│     ├─ weather-wind/                       │
│     │  ├─ 01.avro                          │
│     │  ├─ 02.avro                          │
│     │  └─ 12.avro  (Current month)         │
│     └─ ...                                 │
│                                            │
│  Each file contains:                       │
│  • All stations for that month             │
│  • All hourly observations                 │
│  • Deduplicated by (stationId, time)       │
│  • Sorted by timeObserved, stationId       │
└────────────────────────────────────────────┘
         │
         ▼
    Analytics & Queries
    • Monthly aggregations
    • Station-level analysis
    • Long-term trends
```

---

## Key Features

### 1. Monthly File Management with Deduplication

**Strategy:**
- One Avro file per month per weather type
- Read existing → Merge new data → Deduplicate → Overwrite
- Prevents data duplication from reprocessing

**Deduplication Logic:**
```python
# Key: (stationId, timeObserved)
# Latest batchId wins when duplicates exist

window_spec = Window.partitionBy("stationId", "timeObserved") \
                    .orderBy(col("batchId").desc())

deduplicated = combined_df \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1)
```

**Example Scenario:**
```
Existing file: December 2025 (10,000 records, batchId="batch-001")
New data:      December 2025 (3,000 records, batchId="batch-002")

Process:
1. Read existing 10,000 records
2. Union with new 3,000 records = 13,000 total
3. Deduplicate by (stationId, timeObserved)
   → Overlaps: keep batch-002 (newer)
   → New stations: keep all
4. Result: ~12,000 unique records
5. Overwrite December file
```

### 2. Streaming Read + Batch Write Pattern

**Read:** Kafka streaming (micro-batches every 60s)
**Write:** Batch overwrite of monthly files

**Why This Approach?**
- ✅ Stream processing for real-time enrichment
- ✅ Monthly files for efficient analytics queries
- ✅ Automatic deduplication on every batch
- ✅ No small file problem (one file per month)

### 3. Schema Evolution with Avro

**Input Schema** (from Kafka):
```json
{
  "fields": [
    {"name": "timeObserved", "type": "string"},
    {"name": "stationId", "type": "string"},
    {"name": "stationName", "type": "string"},
    {"name": "lon", "type": "double"},
    {"name": "lat", "type": "double"},
    {"name": "value", "type": "double"},
    {"name": "valueFieldName", "type": "string"},
    {"name": "batchId", "type": "string"}
  ]
}
```

**Output Schema** (enriched, in HDFS):
```json
{
  "fields": [
    {"name": "timeObserved", "type": "string"},
    {"name": "stationId", "type": "string"},
    {"name": "stationName", "type": "string"},
    {"name": "mean_wind_speed", "type": "double"},    // value renamed
    {"name": "lon", "type": "double"},
    {"name": "lat", "type": "double"},
    {"name": "dkArea", "type": "int"},                // ← ADDED
    {"name": "municipalityCode", "type": "int"}       // ← ADDED
  ]
}
```

**Field Renaming:**
```python
TOPIC_TO_PARAM = {
    "historical-weather-wind": "mean_wind_speed",
    "historical-weather-temp": "mean_temp",
    "historical-weather-sun": "mean_radiation"
}

# Generic "value" field → Specific parameter name
.withColumn(param_name, col("value"))
```

### 4. Municipality Enrichment

**CSV Format:** `/utils/municipality_codes_to_coordinates.csv`
```csv
code,latitude,longitude
101,55.6761,12.5683    # København
147,55.3959,10.3883    # Fredericia
...
(311 municipalities - includes historical codes)
```

**Enrichment Algorithm:**
```python
def find_nearest_municipality(lat, lon):
    """Find nearest municipality using Euclidean distance"""
    min_distance = float('inf')
    nearest_code = None
    
    for muni in municipalities:
        distance = sqrt((lat - muni.lat)² + (lon - muni.lon)²)
        if distance < min_distance:
            min_distance = distance
            nearest_code = muni.code
    
    return nearest_code
```

**DK Area Calculation:**
```python
def calculate_dk_area(lon):
    """Denmark is split into two areas"""
    return 1 if lon <= 12.0 else 2
    # DK1 (West): Jutland + Funen (lon <= 12°)
    # DK2 (East): Zealand + Bornholm (lon > 12°)
```

### 5. Avro Deserialization Fix

**Problem:** Confluent Schema Registry adds 5-byte header to Avro messages
```
Byte 0:     Magic byte (0x00)
Bytes 1-4:  Schema ID (big-endian int)
Bytes 5+:   Actual Avro data
```

**Solution:** Strip header before deserialization
```python
# BEFORE (fails):
from_avro(col("value"), schema_str)

# AFTER (works):
from_avro(
    expr("substring(value, 6, length(value)-5)"),  # Skip first 5 bytes
    schema_str
)
```

### 6. Cache-Based Race Condition Prevention

**Problem:** Deleting files while Spark still needs to read them
```python
# WRONG - causes race condition:
existing_df = spark.read.format("avro").load(path)  # Lazy
combined_df = existing_df.union(new_df)             # Still lazy
delete_hdfs_path(path)                              # Delete source!
combined_df.write.save(path)                        # ERROR: Files gone!
```

**Solution:** Cache before delete
```python
# CORRECT - materialized before delete:
existing_df = spark.read.format("avro").load(path)
combined_df = existing_df.union(new_df)
combined_df = combined_df.cache()                   # Force materialization
count = combined_df.count()                         # Trigger execution
delete_hdfs_path(path)                              # Now safe to delete
combined_df.write.save(path)                        # Reads from cache
combined_df.unpersist()                             # Free memory
```

---

## Deployment

### 1. Build and Push Docker Image

```bash
# Build for AMD64 (cluster architecture)
docker build --platform linux/amd64 \
  -t registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/kafka-historical-enricher:latest \
  -f Dockerfile-historical .

# Push to registry
docker push registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/kafka-historical-enricher:latest
```

### 2. Kubernetes Deployment

**Apply configuration:**
```bash
kubectl apply -f kafka-historical-enricher.yaml
```

**Deployment manifest (kafka-historical-enricher.yaml):**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-historical-weather-enricher
  namespace: bd-bd-gr-05
spec:
  replicas: 1  # MUST BE 1 (stateful streaming)
  selector:
    matchLabels:
      app: kafka-historical-weather-enricher
  template:
    metadata:
      labels:
        app: kafka-historical-weather-enricher
    spec:
      serviceAccountName: spark
      containers:
      - name: spark-historical-enricher
        image: registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/kafka-historical-enricher:latest
        imagePullPolicy: Always
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        env:
        - name: BOOTSTRAP_SERVERS
          value: "kafka-g5-controller-headless:9092"
        - name: SCHEMA_REGISTRY_URL
          value: "http://schema-registry:8081"
        - name: HDFS_NAMENODE
          value: "hdfs://namenode-g5:9000"
        - name: CHECKPOINT_ROOT
          value: "/tmp/spark/checkpoints/historical_enricher_v4"
        - name: TRIGGER_INTERVAL
          value: "60 seconds"
        - name: MUNICIPALITY_CSV_HDFS
          value: "hdfs://namenode-g5:9000/utils/municipality_codes_to_coordinates.csv"
```

### 3. Configuration Parameters

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `BOOTSTRAP_SERVERS` | `kafka-g5:9092` | Kafka broker addresses |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | Schema Registry endpoint |
| `HDFS_NAMENODE` | `hdfs://namenode-g5:9000` | HDFS namenode URI |
| `CHECKPOINT_ROOT` | `/tmp/spark/checkpoints/historical_enricher_v4` | Spark streaming checkpoint location |
| `TRIGGER_INTERVAL` | `60 seconds` | Micro-batch interval |
| `MUNICIPALITY_CSV_HDFS` | `hdfs://namenode-g5:9000/utils/...` | Municipality mapping file |

**Checkpoint Versioning:**
- Change version (v4 → v5) when modifying:
  - Schema transformations
  - Processing logic
  - UDF implementations
- Keeps old checkpoints isolated
- Forces clean restart from "earliest" offsets

---

## Monitoring

### 1. Pod Status

```bash
# Check pod status
kubectl get pods -n bd-bd-gr-05 -l app=kafka-historical-weather-enricher

# Expected output:
NAME                                                READY   STATUS    RESTARTS   AGE
kafka-historical-weather-enricher-xxxxxxxxxx-xxxxx  1/1     Running   0          5m

# Describe pod for events
kubectl describe pod -n bd-bd-gr-05 -l app=kafka-historical-weather-enricher
```

### 2. Application Logs

```bash
# Follow live logs
kubectl logs -f deployment/kafka-historical-weather-enricher -n bd-bd-gr-05

# Recent logs (last 100 lines)
kubectl logs --tail=100 deployment/kafka-historical-weather-enricher -n bd-bd-gr-05

# Logs from previous pod (if crashed)
kubectl logs --previous deployment/kafka-historical-weather-enricher -n bd-bd-gr-05
```

**Key Log Patterns:**

**✅ Successful Startup:**
```
HISTORICAL WEATHER DATA SPARK CONSUMER
Topics: historical-weather-wind, historical-weather-temp, historical-weather-sun
HDFS: hdfs://namenode-g5:9000/historical/<year>/<weather-type>/<month>.avro
Checkpoint: /tmp/spark/checkpoints/historical_enricher_v4

📍 Initializing municipality lookup...
✓ Successfully loaded 311 municipalities from HDFS

🚀 STARTING STREAM FOR: historical-weather-wind
✓ Successfully fetched schema for historical-weather-wind-value
✓ Stream configured for historical-weather-wind
✓ Enrichment UDFs applied
✓ Stream started: historical-weather-wind -> HDFS
```

**✅ Batch Processing:**
```
========== BATCH 5 START (historical-weather-wind) ==========
📥 Received 3,000 records
📅 Processing 1 year/month combination(s)

📁 Processing 2025-12...
  📊 New records: 3,000
  📂 Found existing data at hdfs://.../2025/weather-wind/12.avro
  📊 Existing records: 10,335
  🔗 Combined with existing data
  ✓ After deduplication: 12,282 records
  🗑️  Deleted: hdfs://.../2025/weather-wind/12.avro
  💾 Written to HDFS: hdfs://.../2025/weather-wind/12.avro (3.45s)

✅ BATCH 5 COMPLETED in 8.23s
   └─ Total records in HDFS: 12,282
```

**❌ Error Patterns:**
```
# Kafka connection failed
org.apache.kafka.common.errors.TimeoutException: 
  Failed to update metadata after 60000 ms.

# HDFS connection failed
java.net.ConnectException: Call From ... to namenode-g5:9000 failed

# Schema Registry unavailable
Failed to fetch schema for historical-weather-wind-value: 
  Connection refused

# Avro deserialization error (missing header strip)
org.apache.avro.AvroRuntimeException: Malformed data. Length is negative

# Race condition error (missing cache)
org.apache.spark.SparkFileNotFoundException: File does not exist
```

### 3. Resource Usage

```bash
# CPU and memory usage
kubectl top pod -n bd-bd-gr-05 -l app=kafka-historical-weather-enricher

# Expected (under load):
NAME                                                CPU(cores)   MEMORY(bytes)
kafka-historical-weather-enricher-xxx-xxx           800m         2.5Gi
```

### 4. Spark UI (Optional)

```bash
# Port-forward Spark UI
kubectl port-forward -n bd-bd-gr-05 \
  deployment/kafka-historical-weather-enricher 4040:4040

# Access at: http://localhost:4040
```

**Key Metrics in Spark UI:**
- **Streaming tab:** 
  - Batch processing times
  - Input rates per topic
  - Scheduling delays
- **Executors tab:** 
  - Memory usage
  - Task distribution
  - GC time
- **SQL tab:** 
  - Query plans for deduplication
  - Shuffle metrics

---

## Verification

### 1. Kafka Topic Status

```bash
# List historical topics
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- \
  kafka-topics.sh --bootstrap-server localhost:9092 --list | grep historical

# Check topic lag (consumer group)
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group historical-weather-enricher

# Expected output shows current offset and lag
TOPIC                        PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
historical-weather-wind      0          125000          125000          0
historical-weather-temp      0          98000           98000           0
historical-weather-sun       0          82000           82000           0
```

### 2. HDFS Output Verification

```bash
# List year directories
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/

# Expected:
drwxr-xr-x   - sparkuser supergroup  0 2024-01-15 10:30 /historical/2024
drwxr-xr-x   - sparkuser supergroup  0 2025-01-01 00:00 /historical/2025

# List weather type directories for a year
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/2025/

# Expected:
drwxr-xr-x   - sparkuser supergroup  0 2025-12-11 18:00 /historical/2025/weather-wind
drwxr-xr-x   - sparkuser supergroup  0 2025-12-11 18:00 /historical/2025/weather-temp
drwxr-xr-x   - sparkuser supergroup  0 2025-12-11 18:00 /historical/2025/weather-sun

# List monthly files for a weather type
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/2025/weather-wind/

# Expected:
-rw-r--r--   3 sparkuser supergroup  1.2M 2025-01-31 23:59 /historical/2025/weather-wind/01.avro
-rw-r--r--   3 sparkuser supergroup  1.1M 2025-02-28 23:59 /historical/2025/weather-wind/02.avro
...
-rw-r--r--   3 sparkuser supergroup  845K 2025-12-11 18:26 /historical/2025/weather-wind/12.avro

# Check file size and record count
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -du -h /historical/2025/weather-wind/12.avro
```

### 3. Data Quality Checks

**Using Spark Shell:**
```bash
# Start Spark shell with Avro support
kubectl exec -it kafka-historical-weather-enricher-xxx-xxx -n bd-bd-gr-05 -- \
  spark-shell --packages org.apache.spark:spark-avro_2.12:3.4.1

# Read and inspect data
val df = spark.read.format("avro")
  .load("hdfs://namenode-g5:9000/historical/2025/weather-wind/12.avro")

// Check schema
df.printSchema()

// Count records
df.count()

// Show sample data
df.show(10, truncate=false)

// Check for nulls in enrichment columns
df.filter("dkArea is null OR municipalityCode is null").count()
// Should return 0

// Verify municipality codes are valid (101-851)
df.filter("municipalityCode < 101 OR municipalityCode > 851").count()
// Should return 0

// Verify dkArea values (1 or 2 only)
df.filter("dkArea NOT IN (1, 2)").count()
// Should return 0

// Check deduplication (no duplicate stationId + timeObserved)
df.groupBy("stationId", "timeObserved").count()
  .filter("count > 1").count()
// Should return 0

// Check temporal coverage
df.selectExpr("min(timeObserved)", "max(timeObserved)").show()

// Check spatial coverage
df.selectExpr("count(distinct stationId) as stations").show()
```

**Expected Schema:**
```
root
 |-- timeObserved: string (nullable = true)
 |-- stationId: string (nullable = true)
 |-- stationName: string (nullable = true)
 |-- mean_wind_speed: double (nullable = true)  // or mean_temp, mean_radiation
 |-- lon: double (nullable = true)
 |-- lat: double (nullable = true)
 |-- dkArea: integer (nullable = true)
 |-- municipalityCode: integer (nullable = true)
```

### 4. End-to-End Pipeline Test

```bash
# 1. Check if producer has sent new data
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- \
  kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic historical-weather-wind --max-messages 1 \
  --property print.timestamp=true

# 2. Verify enricher is consuming
kubectl logs --tail=50 deployment/kafka-historical-weather-enricher -n bd-bd-gr-05 | grep "Received"

# 3. Check latest HDFS write
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls -t /historical/2025/weather-wind/ | head -2

# 4. Verify record count increased
# (Compare with previous count)
```

---

## Quick Reference

### Essential Commands

```bash
# === Deployment ===
kubectl apply -f kafka-historical-enricher.yaml
kubectl rollout restart deployment/kafka-historical-weather-enricher -n bd-bd-gr-05
kubectl rollout status deployment/kafka-historical-weather-enricher -n bd-bd-gr-05

# === Monitoring ===
kubectl logs -f deployment/kafka-historical-weather-enricher -n bd-bd-gr-05
kubectl logs --tail=100 deployment/kafka-historical-weather-enricher -n bd-bd-gr-05
kubectl top pod -n bd-bd-gr-05 | grep historical

# === Kafka Verification ===
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- \
  kafka-topics.sh --bootstrap-server localhost:9092 --list | grep historical

kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group historical-weather-enricher

# === HDFS Verification ===
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/2025/weather-wind/

kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -du -h /historical/2025/weather-wind/12.avro

# === Debugging ===
kubectl describe pod -n bd-bd-gr-05 -l app=kafka-historical-weather-enricher
kubectl get events -n bd-bd-gr-05 --sort-by='.lastTimestamp' | tail -20

# === Clean Restart (if needed) ===
kubectl delete deployment kafka-historical-weather-enricher -n bd-bd-gr-05
kubectl apply -f kafka-historical-enricher.yaml
```

### Important Paths

| Path | Type | Purpose |
|------|------|---------|
| `/historical/YYYY/weather-{type}/MM.avro` | HDFS File | Monthly weather data (deduplicated) |
| `/utils/municipality_codes_to_coordinates.csv` | HDFS File | Municipality mapping (311 codes) |
| `/tmp/spark/checkpoints/historical_enricher_v4/` | Local/HDFS | Spark streaming checkpoints |
| `historical-weather-{wind\|temp\|sun}` | Kafka Topic | Input topics with raw historical data |

### Key Metrics to Monitor

| Metric | Command | Healthy Range |
|--------|---------|---------------|
| **Pod Status** | `kubectl get pods \| grep historical` | Running, 0 restarts |
| **Memory Usage** | `kubectl top pod \| grep historical` | < 3.5Gi (limit 4Gi) |
| **CPU Usage** | `kubectl top pod \| grep historical` | < 1.5 cores (limit 2) |
| **Kafka Lag** | `kafka-consumer-groups --describe` | < 1000 messages |
| **Batch Duration** | Check logs for "COMPLETED in Xs" | < 30 seconds |
| **Processing Rate** | Check logs for "Process Rate" | > 100 rows/sec |

---

## Troubleshooting

### Issue 1: Avro Deserialization Error

**Symptoms:**
```
org.apache.avro.AvroRuntimeException: Malformed data. Length is negative: -49
```

**Cause:** Missing Confluent Schema Registry header stripping

**Solution:**
```python
# BEFORE (incorrect):
decoded_df = raw_df.select(
    from_avro(col("value"), schema_str).alias("data")
)

# AFTER (correct):
decoded_df = raw_df.select(
    from_avro(
        expr("substring(value, 6, length(value)-5)"),  # Strip 5-byte header
        schema_str
    ).alias("data")
)
```

### Issue 2: File Not Found Race Condition

**Symptoms:**
```
org.apache.spark.SparkFileNotFoundException: File does not exist: 
hdfs://namenode-g5:9000/historical/2025/weather-wind/12.avro/part-00005-xxx.avro
```

**Cause:** Deleting files before Spark finishes reading them (lazy evaluation)

**Solution:**
```python
# Add caching before delete
deduplicated_df = deduplicated_df.cache()
final_count = deduplicated_df.count()  # Trigger materialization
delete_hdfs_path(spark, hdfs_path)     # Now safe
deduplicated_df.write.save(hdfs_path)
deduplicated_df.unpersist()            # Clean up
```

### Issue 3: Consumer Not Processing (0 rows/sec)

**Symptoms:**
- Logs show streams starting but no batches processing
- Input rate: 0.00 rows/sec

**Possible Causes:**
1. Checkpoint offset stuck at "latest" instead of "earliest"
2. No new data in Kafka topics
3. Kafka connection issues

**Solutions:**

**A. Reset checkpoint (force read from earliest):**
```bash
# Change checkpoint version in deployment
CHECKPOINT_ROOT: "/tmp/spark/checkpoints/historical_enricher_v5"  # v4 -> v5

# Redeploy
kubectl apply -f kafka-historical-enricher.yaml
```

**B. Verify Kafka has data:**
```bash
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- \
  kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic historical-weather-wind

# Should show: historical-weather-wind:0:125000 (non-zero end offset)
```

**C. Check Kafka connectivity:**
```bash
kubectl logs deployment/kafka-historical-weather-enricher -n bd-bd-gr-05 | grep -i kafka
# Look for connection errors
```

### Issue 4: Schema Registry Connection Failed

**Symptoms:**
```
Failed to fetch schema for historical-weather-wind-value: Connection refused
```

**Solutions:**

**A. Verify Schema Registry is running:**
```bash
kubectl get pods -n bd-bd-gr-05 | grep schema-registry
kubectl logs -n bd-bd-gr-05 schema-registry-xxx-xxx --tail=50
```

**B. Test connectivity from enricher pod:**
```bash
kubectl exec -it kafka-historical-weather-enricher-xxx-xxx -n bd-bd-gr-05 -- \
  curl http://schema-registry:8081/subjects
```

**C. Check service endpoint:**
```bash
kubectl get svc -n bd-bd-gr-05 | grep schema-registry
```

### Issue 5: Out of Memory Errors

**Symptoms:**
```
java.lang.OutOfMemoryError: Java heap space
Container killed: OOMKilled
```

**Solutions:**

**A. Increase memory limits:**
```yaml
resources:
  requests:
    memory: "3Gi"    # Was 2Gi
  limits:
    memory: "6Gi"    # Was 4Gi
```

**B. Reduce batch size:**
```yaml
env:
- name: MAX_OFFSETS_PER_TRIGGER
  value: "10000"    # Reduce from 50000
```

**C. Tune Spark memory:**
```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "3g") \
    .config("spark.executor.memory", "3g") \
    .config("spark.sql.shuffle.partitions", "8")  # Reduce from default
```

### Issue 6: HDFS Write Failures

**Symptoms:**
```
java.io.IOException: Failed to write to HDFS
org.apache.hadoop.ipc.RemoteException: Operation category WRITE is not supported
```

**Solutions:**

**A. Check HDFS safemode:**
```bash
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfsadmin -safemode get
# If in safemode: hdfs dfsadmin -safemode leave
```

**B. Verify HDFS space:**
```bash
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -df -h /
```

**C. Check directory permissions:**
```bash
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls -R /historical/2025/

# Fix if needed:
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -chmod -R 755 /historical/
```

### Issue 7: Municipality Lookup Failures

**Symptoms:**
```
municipalityCode: null (for all records)
Could not read municipality CSV
```

**Solutions:**

**A. Verify CSV exists:**
```bash
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /utils/municipality_codes_to_coordinates.csv

# If missing, upload it
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -put municipality_codes_to_coordinates.csv /utils/
```

**B. Check CSV format:**
```bash
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -cat /utils/municipality_codes_to_coordinates.csv | head -5

# Should show:
# code,latitude,longitude
# 101,55.6761,12.5683
# 147,55.3959,10.3883
```

---

## Notes

### Critical Configuration Rules

- **Single Replica Only:** Must run as `replicas: 1` (stateful streaming with checkpoints)
- **Checkpoint Versioning:** Bump version when changing:
  - Schema transformations
  - Processing logic
  - UDF implementations
  - Kafka offset strategy
- **Memory Requirements:** 
  - Minimum: 2Gi
  - Recommended: 4Gi
  - Heavy load: 6Gi+
- **Batch Interval:** 60 seconds default, adjust based on:
  - Data volume per topic
  - Deduplication overhead
  - HDFS write latency

### Data Processing Guarantees

- **Exactly-once semantics** within each batch (via deduplication)
- **At-least-once delivery** from Kafka (may process same data multiple times)
- **Idempotent writes** to HDFS (deduplication handles reprocessing)
- **No data loss** (checkpointing + earliest offset strategy)

### Performance Characteristics

**Expected Throughput:**
- **Input:** 3,000-5,000 records/batch (per topic)
- **Processing:** 100-500 rows/sec
- **Latency:** 5-15 seconds per batch
- **Storage Growth:** ~1-2 MB per month per weather type

**Bottlenecks:**
1. **Deduplication shuffle** (partitionBy + orderBy)
2. **HDFS read** (existing monthly files)
3. **HDFS write** (overwrite mode)
4. **Municipality lookup** (broadcast join - optimized)

### Maintenance Tasks

**Weekly:**
- Check disk usage: `hdfs dfs -du -h /historical/`
- Review error logs: `kubectl logs --since=7d | grep ERROR`
- Verify deduplication: Query for duplicate keys

**Monthly:**
- Verify monthly file completeness (all 12 months present)
- Check for data gaps (missing dates/stations)
- Archive old checkpoint directories

**Quarterly:**
- Review and tune resource limits
- Analyze query performance patterns
- Update municipality CSV if codes change

### Data Retention Policy

Current setup keeps **all historical data indefinitely**. Consider:

**Option A: Partition by year**
- Keep last 2 years in `/historical/`
- Archive older years to `/archive/YYYY/`

**Option B: Compression**
- HDFS supports transparent compression
- Avro files can use Snappy/GZIP codecs

**Option C: Aggregation**
- Keep hourly data for 1 year
- Aggregate to daily for older data

### Related Components

| Component | Purpose | Documentation |
|-----------|---------|---------------|
| **Historical Producer** | Backfills historical data to Kafka | `historical-producer/README.md` |
| **Forecast Enricher** | Processes live forecast data | `KafkaEnrichmentGuide.md` |
| **Schema Registry** | Manages Avro schemas | Confluent Schema Registry docs |
| **HDFS** | Distributed storage | Hadoop HDFS docs |

### Future Enhancements

**Potential improvements:**
1. **Partitioned writes** by year/month for faster queries
2. **Incremental processing** (read only new data, not full month)
3. **Compaction** of small batches into larger files
4. **Streaming aggregations** (hourly/daily rollups)
5. **Quality metrics** (completeness, freshness, accuracy)
6. **Alert system** for processing failures/delays
7. **Dashboard** for monitoring pipeline health

---

## Support

**Logs Location:**
- Application: `kubectl logs deployment/kafka-historical-weather-enricher -n bd-bd-gr-05`
- Spark UI: `kubectl port-forward deployment/kafka-historical-weather-enricher 4040:4040`

**Contact:**
- Team: The European Avengers
- Project: Big Data Course - Historical Weather Pipeline

**Documentation:**
- This guide: `HistoricalWeatherEnrichmentGuide.md`
- Code: `consumer-historical.py`, `enrichers.py`
- Deployment: `kafka-historical-enricher.yaml`
- Dockerfile: `Dockerfile-historical`

---

*Last updated: 2025-12-11*
*Pipeline version: v4*
*Spark version: 3.4.1*
*Avro version: 1.11.0*
