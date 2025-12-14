# Batch Production Calculation - Green Energy

Batch Spark job that calculates green energy production (wind + solar) from historical weather data and writes monthly AVRO files to HDFS.

## What It Does

Reads weather data (wind speed and solar radiation), combines it with installed capacity data (solar panels and wind mills), and calculates hourly production for each municipality:

- **Calculates** wind production using capacity factor curves based on wind speed
- **Calculates** solar production from radiation with 15% panel efficiency
- **Aggregates** multiple weather predictions per municipality (averages non-zero values)
- **Writes** monthly production data to `/historical/{YEAR}/production/{MONTH}.avro`
- **Runs continuously** checking for new weather data every 6 hours

### Input

**Weather Data (HDFS):**
```
/historical/{YEAR}/
├── weather-wind/{MM}.avro    (mean_wind_speed in m/s)
└── weather-sun/{MM}.avro     (mean_radiation in W/m²)
```

**Capacity Data (HDFS):**
```
/utils/
├── solar_panels.csv          (komnr, kw_total, kommune)
├── wind_mills.csv            (Kommune, Installed capacity [kW])
└── municipality_codes_to_coordinates.csv
```

### Processing

1. **First Run:** Processes all available historical weather data
2. **Subsequent Runs:** Only processes data with timestamps after last run (tracked in state file)
3. **Aggregation:** If multiple weather stations report for same municipality-time, averages non-zero values
4. **Production Calculation:**
   - **Wind:** `capacity_kw × capacity_factor(wind_speed) × 1 hour`
   - **Solar:** `capacity_kw × (radiation / 1000) × 0.15 × 1 hour`
   - **Total:** Sum of wind + solar
5. **Deduplication:** Merges with existing monthly files, keeps latest records

### Output
```
/historical/
├── 2022/production/
│   ├── 08.avro    (August 2022 production)
│   └── ...
├── 2023/production/
│   ├── 01.avro
│   └── ...
└── 2025/production/
    └── 01.avro
```

**Schema:**
```
timeObserved (timestamp)
municipalityCode (int)
dkArea (int)
windProductionKwh (double)
sunProductionKwh (double)
productionKwh (double)    // Sum of wind + solar
```

## How It Works

### Wind Production Formula

**Capacity Factor Curve:**
```python
wind_speed < 3 m/s    → 0%
3-4 m/s               → 5%
4-5 m/s               → 12%
5-6 m/s               → 22%
6-7 m/s               → 35%
7-8 m/s               → 50%
8-9 m/s               → 65%
9-10 m/s              → 80%
10-12 m/s             → 95%
12-25 m/s             → 90%
> 25 m/s              → 0% (cut-off)
```

**Production:** `wind_capacity_kw × capacity_factor × 1 hour = kWh`

### Solar Production Formula

**Given:**
- `mean_radiation` in W/m² (hourly average)
- Panel efficiency: 15%

**Production:** `solar_capacity_kw × (radiation / 1000) × 0.15 × 1 hour = kWh`

### State Tracking

**File:** `/utils/production_job_state.txt` in HDFS

Stores last processed timestamp to enable incremental processing. On each run:
1. Read last timestamp (if exists)
2. Process only weather data after this timestamp
3. Save new maximum timestamp after successful processing

## Architecture

```
┌─────────────────────────┐
│   Weather Data (HDFS)   │
│   /historical/YYYY/     │
│   • weather-wind/       │
│   • weather-sun/        │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│  Batch Production Job               │
│  (Spark - runs every 6h)            │
│                                     │
│  ┌────────────────────────────┐     │
│  │ Capacity Lookup            │     │
│  │ /utils/solar_panels.csv    │     │
│  │ /utils/wind_mills.csv      │     │
│  └────────────────────────────┘     │
│                                     │
│  Components:                        │
│  • DataReader: Load weather + cap   │
│  • Calculator: Compute production   │
│  • DataWriter: Save to HDFS         │
│                                     │
│  State:                             │
│  • Last timestamp in state file     │
│  • Sleep 6h between runs            │
└──────────┬──────────────────────────┘
           │
           ▼
┌─────────────────────────┐
│   Production Data       │
│   /historical/YYYY/     │
│   production/MM.avro    │
│                         │
│   Monthly files with:   │
│   • Wind production     │
│   • Solar production    │
│   • Total production    │
│   • By municipality     │
└─────────────────────────┘
```

## Quick Start

### Prerequisites

```bash
# Verify capacity data exists in HDFS
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /utils/solar_panels.csv

kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /utils/wind_mills.csv

# If missing, upload:
kubectl cp solar_panels.csv bd-bd-gr-05/namenode-g5-0:/tmp/
kubectl cp wind_mills.csv bd-bd-gr-05/namenode-g5-0:/tmp/

kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -put /tmp/solar_panels.csv /utils/

kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -put /tmp/wind_mills.csv /utils/
```

### Deploy

```bash
# 1. Build Docker image
docker build --platform linux/amd64 \
  -t registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/batch-production-calculation:latest .

# 2. Push to registry
docker push registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/batch-production-calculation:latest

# 3. Deploy to Kubernetes
kubectl apply -f batch-production-deployment.yaml

# 4. Monitor logs
kubectl logs -f deployment/batch-production-calculation -n bd-bd-gr-05
```

## Monitoring

### Check Logs

```bash
# Follow logs in real-time
kubectl logs -f deployment/batch-production-calculation -n bd-bd-gr-05

# View recent logs
kubectl logs --tail=100 deployment/batch-production-calculation -n bd-bd-gr-05

# Check for errors
kubectl logs deployment/batch-production-calculation -n bd-bd-gr-05 | grep ERROR
```

**Expected Log Output:**
```
============================================================
Starting processing run at 2025-12-11 18:30:00
============================================================

📅 Last processed timestamp: 2025-01-31 23:00:00

============================================================
Processing Green Energy Production Data
Processing data after: 2025-01-31 23:00:00
============================================================

📖 [Step 1/4] Loading capacity data...
    ✓ Loaded 98 solar capacity entries
    ✓ Loaded 87 wind capacity entries
✓ Capacity data loaded in 2.3s

📖 [Step 2/4] Reading weather data from HDFS...
  Found 12 year-month combinations to process
✓ Loaded 125,340 weather records in 8.7s

🔧 [Step 3/4] Calculating green energy production...
  Aggregating weather data by municipality and time...
  Calculating wind energy production...
  Calculating solar energy production...
  ✓ Production calculation complete
✓ Calculated production for 98,450 records in 15.2s

💾 [Step 4/4] Writing production data to HDFS...
  Writing data for 3 year-month combinations...
    [1/3] Writing 2025-01: 8,234 records...
      ✓ Completed in 3.1s → hdfs://namenode-g5:9000/historical/2025/production/01.avro
✓ Production data written in 9.5s

✅ Processing completed successfully in 0m 35s
✓ Saved last processed timestamp: 2025-01-31 23:00:00

😴 Sleeping until next run at 2025-12-12 18:30:00
   (waiting 6 hours...)
```

### Check Pod Status

```bash
# View pod status
kubectl get pods -n bd-bd-gr-05 | grep batch-production

# Describe pod for details
kubectl describe pod -l app=batch-production-calculation -n bd-bd-gr-05

# Check resource usage
kubectl top pod -n bd-bd-gr-05 | grep production
```

### Spark UI

```bash
# Port-forward Spark UI
kubectl port-forward -n bd-bd-gr-05 \
  deployment/batch-production-calculation 4040:4040

# Access at: http://localhost:4040
```

## Verification

### Check HDFS Output

```bash
# List production directories
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/2025/production/

# Expected output:
-rw-r--r--  3  sparkuser  supergroup  850K  2025-01-31  /historical/2025/production/01.avro
-rw-r--r--  3  sparkuser  supergroup  780K  2025-02-28  /historical/2025/production/02.avro

# Check file size
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -du -h /historical/2025/production/
```

### Verify State File

```bash
# Check last processed timestamp
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -cat /utils/production_job_state.txt

# Should show ISO timestamp like: 2025-01-31T23:00:00
```

### Data Quality Check

```bash
# Start Spark shell
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- spark-shell \
  --packages org.apache.spark:spark-avro_2.12:3.4.1

# In Spark shell:
val df = spark.read.format("avro")
  .load("hdfs://namenode-g5:9000/historical/2025/production/01.avro")

df.printSchema()
// Should show: timeObserved, municipalityCode, dkArea, 
//              windProductionKwh, sunProductionKwh, productionKwh

df.count()
// Check record count

df.show(10, false)
// View sample data

// Check for nulls
df.filter("windProductionKwh is null OR sunProductionKwh is null").count()
// Should be 0

// Verify total = wind + solar
df.filter("abs(productionKwh - (windProductionKwh + sunProductionKwh)) > 0.01").count()
// Should be 0
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HDFS_NAMENODE` | `hdfs://namenode-g5:9000` | HDFS namenode URI |
| `SLEEP_INTERVAL_HOURS` | `6` | Hours between processing runs |

**Adjust sleep interval in deployment YAML:**
```yaml
env:
- name: SLEEP_INTERVAL_HOURS
  value: "12"  # Run every 12 hours instead of 6
```

### Resource Configuration

```yaml
resources:
  requests:
    cpu: "2"
    memory: "4Gi"
  limits:
    cpu: "4"
    memory: "8Gi"
```

Increase for larger datasets or faster processing.

## Troubleshooting

### Issue: No Data Processing (Empty Weather Data)

**Symptoms:**
```
ℹ️  No new weather data to process
```

**Solutions:**
- Check if weather data exists for time range after last timestamp
- Verify weather enrichment jobs have run successfully
- Reset state file to reprocess all data:
```bash
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -rm /utils/production_job_state.txt
```

### Issue: Missing Capacity Data

**Symptoms:**
```
⚠️  Warning: Could not load solar panels data
⚠️  Warning: Could not load wind mills data
```

**Solution:**
Upload capacity CSV files to HDFS (see Prerequisites section)

### Issue: Out of Memory

**Symptoms:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Solutions:**

1. Increase memory limits in deployment YAML:
```yaml
limits:
  memory: "12Gi"  # Increase from 8Gi
```

2. Reduce processing load (process fewer months at once)

### Issue: Production Values Are Zero

**Symptoms:**
All production values are 0 kWh

**Possible Causes:**
1. Municipality codes in weather data don't match capacity data
2. Capacity data missing or incorrect
3. Weather values out of expected range

**Debug:**
```python
# Check municipality code overlap
weather_codes = set(weather_df.select("municipalityCode").distinct().collect())
capacity_codes = set(solar_capacity.keys()).union(set(wind_capacity.keys()))
print("Overlap:", weather_codes.intersection(capacity_codes))
```

## Files

```
batch-production-calculation/
├── src/
│   ├── data_reader.py              # Reads weather & capacity data
│   ├── production_calculator.py    # Calculates production
│   └── data_writer.py              # Writes results to HDFS
├── main.py                         # Orchestrates job (6h loop)
├── Dockerfile
├── batch-production-deployment.yaml
└── requirements.txt
```

## Key Differences from Other Batch Jobs

| Feature | Consumption Enricher | Weather Enricher | **Production Calculator** |
|---------|---------------------|------------------|--------------------------|
| **Input** | CSV files (once) | CSV files (once) | **Weather AVRO (continuous)** |
| **Processing** | Rename + enrich | Enrich + partition | **Calculate production** |
| **Schedule** | One-time | One-time | **Every 6 hours** |
| **State** | None | None | **Timestamp tracking** |
| **Output** | Monthly AVRO | Monthly AVRO | **Monthly AVRO** |
| **Lookup** | Municipality coords | Municipality coords | **Solar + wind capacity** |

## Performance Notes

- **First Run:** 10-30 minutes (processes all historical data)
- **Incremental Runs:** 1-5 minutes (only new data)
- **Memory:** 4-8GB typical
- **CPU:** 2-4 cores recommended
- **Sleep Cycle:** 6 hours (configurable)

## Cleanup

```bash
# Delete deployment
kubectl delete deployment batch-production-calculation -n bd-bd-gr-05
kubectl delete service batch-production-calculation -n bd-bd-gr-05

# Delete production data (optional)
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -rm -r /historical/*/production/

# Delete state file (to reprocess from beginning)
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -rm /utils/production_job_state.txt
```

---

**Namespace:** `bd-bd-gr-05`  
**HDFS Namenode:** `hdfs://namenode-g5:9000`  
**Runtime:** Continuous (6h sleep cycle)