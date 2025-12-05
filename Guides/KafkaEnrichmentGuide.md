# Kafka Enrichment Pipeline - Complete Guide

**Last Updated:** December 5, 2025  
**Version:** v11 (UUID-based forecast cycle management)

## 📋 Table of Contents

1. [Quick Start](#quick-start)
2. [Architecture Overview](#architecture-overview)
3. [Key Features](#key-features)
4. [Deployment](#deployment)
5. [Monitoring](#monitoring)
6. [Verification](#verification)
7. [Quick Reference](#quick-reference)
8. [Notes](#notes)

---

## 🚀 Quick Start

### Prerequisites
- ✅ Kafka cluster running (`kafka-g5-controller-0,1,2`)
- ✅ Schema Registry deployed (`schema-registry:8081`)
- ✅ HDFS namenode accessible (`namenode-g5:9000`)
- ✅ Municipality CSV in HDFS (`/utils/municipality_codes_to_coordinates.csv`)

### Start Pipeline

```bash
# 1. Restart producers (generates new forecast cycle)
kubectl rollout restart deployment/kafka-producer-{1,2,3}-g5 -n bd-bd-gr-05

# 2. Wait for producers to send data (~30 minutes for first cycle)
kubectl logs -f deployment/kafka-producer-1-g5 -n bd-bd-gr-05
# Look for: "✅ FORECAST CYCLE COMPLETED"

# 3. Restart enricher (processes data from Kafka)
kubectl rollout restart deployment/kafka-enricher -n bd-bd-gr-05

# 4. Monitor enricher logs
kubectl logs -f deployment/kafka-enricher -n bd-bd-gr-05
# Look for: "✅ BATCH X COMPLETED"
```

---

## 🏗️ Architecture Overview

```
┌─────────────────┐
│   DMI API       │  Forecast data (wind/temp/sun)
│  (3 producers)  │  Updates every 3 hours
│  + forecastId   │  UUID per cycle: abc-123
└────────┬────────┘
         │ Kafka Topics (Raw)
         │ • weather-wind
         │ • weather-temp  
         │ • weather-sun
         ▼
┌────────────────────────────────┐
│  kafka-enricher                │
│  (Spark Streaming)             │
│  ┌──────────────────────────┐  │
│  │ Municipality Lookup      │  │
│  │ /utils/municipality_...  │  │  98 Danish municipalities
│  │ Nearest-neighbor mapping │  │  Euclidean distance
│  └──────────────────────────┘  │
│                                │
│  Enrichments:                  │
│  + dkArea (1 or 2)             │  Longitude-based region
│  + municipalityCode (101-813)  │  Nearest municipality
│  + forecastId (preserved)      │  UUID cycle tracking
└─────┬──────────────────┬───────┘
      │                  │
      │                  ▼
      │         ┌─────────────────────┐
      │         │ Kafka Enriched      │  Schema Registry
      │         │ (Output Topics)     │  Avro serialization
      │         │ • weather-wind-...  │
      │         │ • weather-temp-...  │
      │         │ • weather-sun-...   │
      │         └─────────────────────┘
      │
      ├─────────────────┬──────────────────┐
      ▼                 ▼                  ▼
┌──────────┐   ┌────────────────┐  ┌────────────────┐
│   Live   │   │  Historical    │  │ Live Archives  │
│  /live/  │   │ /historical/   │  │ /historical/   │
│ forecast/│   │ YYYY/topic/    │  │ live-archives/ │
│ {topic}/ │   │ MM_streaming/  │  │ YYYY/MM/       │
│          │   │ batch-N/       │  │ {topic}_UUID/  │
└──────────┘   └────────────────┘  └────────────────┘
     │                                      │
     │ Current cycle                        │ Completed cycles
     │ APPEND mode                          │ ARCHIVED on UUID change
     │ Rotates every 3h                     │
     ▼                                      ▼
Dashboard/API                        Long-term Analytics
```

---

## 🎯 Key Features

### 1. Triple Sink Architecture
Every batch writes to 3 destinations simultaneously:

| Sink | Path | Mode | Purpose |
|------|------|------|---------|
| **Kafka** | `weather-{param}-enriched` | Produce | Downstream consumers |
| **Live HDFS** | `/live/forecast/{topic}/` | Append | Current 3h cycle (dashboard) |
| **Historical** | `/historical/YYYY/{topic}/MM_streaming/` | Overwrite | Detailed batch tracking |

### 2. UUID-Based Forecast Cycle Management

**Producer Side:**
```python
# Every 3 hours, producer generates new UUID
forecast_id = str(uuid.uuid4())  # e.g., "5d4d7ed2-1860-490c-b032-fe0922930ba4"

# All records in this cycle get same forecastId
record = {
    "lon": lon, "lat": lat, "value": value,
    "step": step, "parameter": parameter,
    "forecastId": forecast_id  # ← NEW
}
```

**Enricher Side:**
```python
# Detect new forecast cycle
if batch_forecast_id != current_forecast_ids[topic]:
    # 1. Archive current live data to historical
    archive_live_file(live_path, old_forecast_id)
    
    # 2. Delete live directory
    delete_hdfs_path(live_path)
    
    # 3. Start fresh with new forecastId
    current_forecast_ids[topic] = batch_forecast_id
```

**Result:**
- Clean file rotation every 3 hours
- No data mixing between forecast cycles
- Historical archive of all cycles for accuracy analysis

### 3. Municipality Enrichment

**CSV Format:** `/utils/municipality_codes_to_coordinates.csv`
```csv
code,latitude,longitude
101,55.6761,12.5683    # København
147,55.3959,10.3883    # Fredericia
...
(98 municipalities)
```


### 4. Schema Evolution with Avro

**Input Schema** (from producer):
```json
{
  "fields": [
    {"name": "lon", "type": "double"},
    {"name": "lat", "type": "double"},
    {"name": "value", "type": "double"},
    {"name": "step", "type": "string"},
    {"name": "parameter", "type": "string"},
    {"name": "forecastId", "type": "string"}
  ]
}
```

**Output Schema** (enriched):
```json
{
  "fields": [
    {"name": "lon", "type": "double"},
    {"name": "lat", "type": "double"},
    {"name": "value", "type": "double"},
    {"name": "step", "type": "string"},
    {"name": "parameter", "type": "string"},
    {"name": "forecastId", "type": "string"},
    {"name": "dkArea", "type": "int"},           // ← ADDED
    {"name": "municipalityCode", "type": "int"}  // ← ADDED
  ]
}
```

---

## 🚢 Deployment

### 1. Build and Push

```bash
docker build --platform linux/amd64 -t registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/kafka-enricher:latest .

docker push registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/kafka-enricher:latest
```

### 2. Kubernetes Deployment

**Apply:**
```bash
kubectl apply -f kafka-enricher.yaml
```


## 📊 Monitoring

### 1. Enricher Logs

```bash
# Follow live logs
kubectl logs -f deployment/kafka-enricher -n bd-bd-gr-05

# Recent logs
kubectl logs --tail=100 deployment/kafka-enricher -n bd-bd-gr-05
```


### 2. Spark UI

```bash
# Port-forward Spark UI
kubectl port-forward -n bd-bd-gr-05 svc/kafka-enricher 4040:4040

# Access at: http://localhost:4040
```

**Key Metrics:**
- **Streaming tab:** Batch processing times, input rates
- **Executors tab:** Memory usage, task distribution
- **SQL tab:** Query plans for each batch



---

## ✅ Verification


### 1. Verify HDFS Output

```bash
# Check live forecast directory
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -ls /live/forecast/

# Expected:
# drwxr-xr-x   - sparkuser supergroup  0 2025-12-05 18:20 /live/forecast/weather-wind
# drwxr-xr-x   - sparkuser supergroup  0 2025-12-05 18:20 /live/forecast/weather-temp
# drwxr-xr-x   - sparkuser supergroup  0 2025-12-05 18:20 /live/forecast/weather-sun

# Check live file size (should be growing)
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -du -h /live/forecast/weather-wind

# Check historical streaming batches
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/2025/weather-wind/12_historical_streaming/ | tail -5

# Check live archives (after 3h cycle completion)
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/live-archives/2025/weather-wind/12_historical_streaming/
```

---

## 📚 Quick Reference

### Essential Commands

```bash
# Start/Restart Pipeline
kubectl rollout restart deployment/kafka-producer-{1,2,3}-g5 -n bd-bd-gr-05
kubectl rollout restart deployment/kafka-enricher -n bd-bd-gr-05

# Monitor
kubectl logs -f deployment/kafka-enricher -n bd-bd-gr-05
kubectl top pod -n bd-bd-gr-05 | grep enricher

# Verify Output
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- kafka-topics.sh --bootstrap-server localhost:9092 --list
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -ls /live/forecast/

# Access UIs
kubectl port-forward -n bd-bd-gr-05 svc/kafka-enricher 4040:4040        # Spark UI
kubectl port-forward -n bd-bd-gr-05 svc/redpanda-console 8080:8080     # Kafka UI
```

### Important Paths

| Path | Type | Purpose |
|------|------|---------|
| `/live/forecast/{topic}/` | HDFS Directory | Current 3h forecast cycle (append mode) |
| `/historical/YYYY/{topic}/MM_streaming/` | HDFS Directory | Detailed batch tracking |
| `/historical/live-archives/YYYY/{topic}/MM_streaming/` | HDFS Directory | Archived 3h cycles |
| `/utils/municipality_codes_to_coordinates.csv` | HDFS File | Municipality mapping (98 codes) |
| `weather-{param}-enriched` | Kafka Topic | Enriched output with dkArea + municipalityCode |

---

## 📝 Notes

- **Checkpoint Versioning:** Always bump `CHECKPOINT_ROOT` version when changing schemas or processing logic
- **Single Replica:** Enricher must run as single replica (stateful streaming with checkpoints)
- **Memory Requirements:** Minimum 2Gi, recommended 4Gi for stable operation
- **Batch Interval:** 30 seconds default, adjust based on data volume
- **Forecast Cycles:** Producer generates new UUID every 3 hours (POLL_INTERVAL=10800s)
- **Archive Timing:** Live data archived when new forecastId detected (automatic)
- **Municipality Broadcast:** Loaded once at startup, cached in memory for performance

**Last Updated:** December 5, 2025 18:30 CET