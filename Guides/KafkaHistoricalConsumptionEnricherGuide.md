# Historical Consumption Enrichment Pipeline

Spark Streaming consumer that processes historical heating consumption data from Kafka, enriches it with DK area information, and saves to monthly HDFS Avro files with automatic deduplication.

## Overview

**Input:** Kafka topic `historical-consumption` with heating consumption records  
**Processing:** Enrich with `dkArea` based on municipality code  
**Output:** Monthly Avro files in `/historical/{YEAR}/consumption/{MM}.avro`  
**Deduplication:** Unique key `(timeUTC, municipalityCode, heatingCategory, housingCategory)`

## Architecture

```
Kafka Topic                     Spark Streaming                    HDFS Output
historical-consumption    →     Consumer + Enricher         →      /historical/YYYY/consumption/MM.avro
                                                                   
Producer runs monthly           - Rename to camelCase              Monthly files:
(2nd of each month)             - Add dkArea enrichment            - 01.avro (January)
                                - Deduplicate                      - 02.avro (February)
Batch upload of                 - Merge with existing              - ...
previous month's data           - Cache before delete              - 12.avro (December)
                                - Overwrite monthly file
```

## Data Flow

### Input Schema (from Kafka)
```
TimeDK, TimeUTC, Municipality, MunicipalityCode, RegionName,
HeatingCategory, HousingCategory, ConsumptionkWh, batchId, yearMonth
```

### Output Schema (in HDFS)
```
timeDK, timeUTC, municipality, municipalityCode, regionName,
heatingCategory, housingCategory, consumptionKwh, batchId, yearMonth, dkArea
```

**Key Changes:**
- ✅ Renamed to camelCase (e.g., `ConsumptionkWh` → `consumptionKwh`)
- ✅ Added `dkArea` (1 if lon < 11, else 2)
- ✅ Removed `year` and `month` columns (implicit in file path)

## Enrichment Logic

**DK Area Calculation:**
```python
# Based on municipality code lookup
municipality_code → (lat, lon) → dkArea
dkArea = 1 if lon < 11 else 2

# DK1 (West): Jutland + Funen (lon < 11°)
# DK2 (East): Zealand + Bornholm (lon >= 11°)
```

**Deduplication Key:**
```
(timeUTC, municipalityCode, heatingCategory, housingCategory)
```
Latest `batchId` wins when duplicates are found.

## Quick Start

### Prerequisites
```bash
# Verify Kafka topic exists
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- \
  kafka-topics.sh --bootstrap-server localhost:9092 --list | grep historical-consumption

# Verify municipality CSV in HDFS
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /utils/municipality_codes_to_coordinates.csv
```

### Deploy

```bash
# Build and deploy (all-in-one)
./build-and-deploy-consumption.sh

# OR manually:

# 1. Build Docker image
docker build --platform linux/amd64 \
  -t registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/kafka-historical-consumption-enricher:latest \
  -f Dockerfile-historical-consumption .

# 2. Push to registry
docker push registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/kafka-historical-consumption-enricher:latest

# 3. Deploy to Kubernetes
kubectl apply -f kafka-historical-consumption-enricher.yaml

# 4. Monitor logs
kubectl logs -f deployment/kafka-historical-consumption-enricher -n bd-bd-gr-05
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BOOTSTRAP_SERVERS` | `kafka-g5-controller-headless:9092` | Kafka brokers |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | Schema Registry endpoint |
| `HDFS_NAMENODE` | `hdfs://namenode-g5:9000` | HDFS namenode URI |
| `CHECKPOINT_ROOT` | `/tmp/spark/.../v1` | Streaming checkpoint location |
| `TRIGGER_INTERVAL` | `1 day` | Micro-batch trigger interval |
| `MUNICIPALITY_CSV_HDFS` | `hdfs://namenode-g5:9000/utils/...` | Municipality lookup CSV |

### Resource Limits

```yaml
resources:
  requests:
    memory: "2Gi"
    cpu: "1000m"
  limits:
    memory: "4Gi"
    cpu: "2000m"
```

Adjust based on data volume.

## Monitoring

### Check Pod Status
```bash
kubectl get pods -n bd-bd-gr-05 -l app=kafka-historical-consumption-enricher
```

### View Logs
```bash
# Follow logs
kubectl logs -f deployment/kafka-historical-consumption-enricher -n bd-bd-gr-05

# Last 100 lines
kubectl logs --tail=100 deployment/kafka-historical-consumption-enricher -n bd-bd-gr-05

# Previous pod (if crashed)
kubectl logs --previous deployment/kafka-historical-consumption-enricher -n bd-bd-gr-05
```

### Resource Usage
```bash
kubectl top pod -n bd-bd-gr-05 -l app=kafka-historical-consumption-enricher
```

### Spark UI
```bash
# Port-forward Spark UI
kubectl port-forward -n bd-bd-gr-05 svc/kafka-historical-consumption-enricher 4040:4040

# Access at: http://localhost:4040
```

## Verification

### Check Kafka Lag
```bash
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group historical-consumption-enricher
```

### Check HDFS Output
```bash
# List year directories
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/

# List monthly files for a year
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/2024/consumption/

# Expected output:
# -rw-r--r--  3  sparkuser  supergroup  2.5M  2024-02-01  /historical/2024/consumption/01.avro
# -rw-r--r--  3  sparkuser  supergroup  2.3M  2024-03-01  /historical/2024/consumption/02.avro
# ...

# Check file size
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -du -h /historical/2024/consumption/
```

### Verify Data Quality
```bash
# Read and inspect data
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- spark-shell \
  --packages org.apache.spark:spark-avro_2.12:3.4.1

# In Spark shell:
val df = spark.read.format("avro").load("hdfs://namenode-g5:9000/historical/2024/consumption/01.avro")
df.printSchema()
df.count()
df.show(10, false)

// Check for nulls in dkArea
df.filter("dkArea is null").count()  // Should be 0

// Verify dkArea values (1 or 2 only)
df.filter("dkArea NOT IN (1, 2)").count()  // Should be 0

// Check deduplication
df.groupBy("timeUTC", "municipalityCode", "heatingCategory", "housingCategory")
  .count()
  .filter("count > 1")
  .count()  // Should be 0
```

## Data Processing Details

### Monthly File Management

1. **Stream reads from Kafka** (micro-batch every 1 day)
2. **Parse timeUTC** to extract year and month
3. **For each year-month:**
   - Read existing monthly file (if exists)
   - Merge with new data
   - Deduplicate by unique key
   - **Cache** result in memory
   - Delete old file
   - Write new file (from cache)
   - Unpersist cache

### Deduplication Strategy

```python
# Unique key: (timeUTC, municipalityCode, heatingCategory, housingCategory)
# Latest batchId wins

Window.partitionBy(
    "timeUTC", "municipalityCode", 
    "heatingCategory", "housingCategory"
).orderBy(col("batchId").desc())
```

**Example:**
```
Existing: timeUTC=2024-01-15 12:00, muni=101, heating=A, housing=B, batchId=batch-001
New:      timeUTC=2024-01-15 12:00, muni=101, heating=A, housing=B, batchId=batch-002

Result: Keep batch-002 (newer)
```

### Cache-Based Race Condition Prevention

**Problem:** Deleting files before Spark finishes reading them

**Solution:**
```python
# 1. Combine existing + new data
combined_df = existing_df.union(new_df)

# 2. Deduplicate
deduplicated_df = combined_df.withColumn(...).filter(...)

# 3. CACHE before delete (materialize in memory)
deduplicated_df = deduplicated_df.cache()
count = deduplicated_df.count()  # Trigger execution

# 4. NOW safe to delete old file
delete_hdfs_path(hdfs_path)

# 5. Write from cache (not from deleted files)
deduplicated_df.write.save(hdfs_path)

# 6. Free memory
deduplicated_df.unpersist()
```

## Troubleshooting

### Issue: Avro Deserialization Error
```
org.apache.avro.AvroRuntimeException: Malformed data. Length is negative
```

**Cause:** Missing Schema Registry header stripping

**Fix:** Already implemented in code:
```python
from_avro(expr("substring(value, 6, length(value)-5)"), schema_str)
```

### Issue: File Not Found Race Condition
```
SparkFileNotFoundException: File does not exist: .../part-00005-xxx.avro
```

**Cause:** Deleting files before Spark reads them

**Fix:** Already implemented with caching (see code above)

### Issue: Consumer Not Processing (0 rows/sec)

**Check 1:** Verify Kafka has data
```bash
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- \
  kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic historical-consumption
```

**Check 2:** Reset checkpoint (force read from earliest)
```bash
# Change checkpoint version in deployment
CHECKPOINT_ROOT: "/tmp/spark/checkpoints/historical_consumption_enricher_v2"  # v1 -> v2

# Redeploy
kubectl apply -f kafka-historical-consumption-enricher.yaml
```

### Issue: Out of Memory
```
java.lang.OutOfMemoryError: Java heap space
```

**Solutions:**

1. Increase memory limits:
```yaml
limits:
  memory: "6Gi"  # Was 4Gi
```

2. Reduce batch size:
```yaml
env:
- name: MAX_OFFSETS_PER_TRIGGER
  value: "10000"  # Reduce from 50000
```

## File Structure

```
.
├── consumer-historical-consumption.py       # Main Spark streaming consumer
├── enrichers.py                             # Enrichment UDFs (shared with weather)
├── Dockerfile-historical-consumption        # Docker image definition
├── entrypoint-historical-consumption.sh     # Spark-submit wrapper
├── kafka-historical-consumption-enricher.yaml  # Kubernetes deployment
├── build-and-deploy-consumption.sh          # Build + deploy script
├── requirements.txt                         # Python dependencies
└── README-consumption.md                    # This file
```

## Expected Output Structure

```
/historical/
├── 2024/
│   └── consumption/
│       ├── 01.avro   (~2-5 MB, all January 2024 data)
│       ├── 02.avro   (~2-5 MB, all February 2024 data)
│       ├── ...
│       └── 12.avro
└── 2025/
    └── consumption/
        ├── 01.avro
        ├── 02.avro
        └── ...
```

**Each monthly file contains:**
- All municipalities (98 codes)
- All heating categories
- All housing categories
- All hours in that month
- Deduplicated records
- Sorted by `(timeUTC, municipalityCode, heatingCategory, housingCategory)`

## Useful Commands

```bash
# === Deployment ===
kubectl apply -f kafka-historical-consumption-enricher.yaml
kubectl rollout restart deployment/kafka-historical-consumption-enricher -n bd-bd-gr-05
kubectl rollout status deployment/kafka-historical-consumption-enricher -n bd-bd-gr-05

# === Monitoring ===
kubectl logs -f deployment/kafka-historical-consumption-enricher -n bd-bd-gr-05
kubectl top pod -n bd-bd-gr-05 | grep consumption

# === Verification ===
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -ls /historical/2024/consumption/
kubectl exec -it kafka-g5-controller-0 -n bd-bd-gr-05 -- \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group historical-consumption-enricher

