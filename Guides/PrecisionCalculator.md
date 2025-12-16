# Batch Precision Calculation - Prediction Accuracy

Batch Spark job that calculates precision metrics for ML prediction accuracy by comparing predicted values with actual historical data. Runs continuously every 6 hours.

## What It Does

Reads prediction files from HDFS archives, joins them with real historical consumption and price data, calculates precision percentages, and writes enriched predictions back with accuracy metrics:

- **Calculates** consumption prediction precision (% accuracy vs. actual consumption)
- **Calculates** price prediction precision (% accuracy vs. actual electricity prices)
- **Enriches** prediction files with precision columns
- **Generates** summary statistics per municipality-year-month
- **Runs continuously** checking for new prediction files every 6 hours

## Data Flow

```
Input (HDFS)                        Processing                          Output (HDFS)
                                                                       
/historical/archives/        →      Spark Job                    →     /historical/archives/
└── {YEAR}/{MM}/                   ├── Load Predictions                └── {YEAR}/{MM}/
    └── analytics/                  ├── Join Real Data                      └── analytics/
        └── predictions_*.          ├── Calculate Precision                     └── predictions_*.parquet
           parquet                  └── Generate Summary                            (+ precision columns)

/historical/{YEAR}/          →      Real Data Source             →     /analytics/
├── consumption/                                                        └── predictions_precision_{YEAR}.parquet
│   └── {MM}.avro                                                          (summary statistics)
└── price.avro
```

## How It Works

### Phase 1: Load Predictions (1-5 min)
1. **Discovers** prediction files in `/historical/archives/{YEAR}/{MM}/analytics/`
2. **Reads** only files starting with `predictions_` (filters out other parquet files)
3. **Filters** by last processed timestamp (tracked in state file)
4. **Adds** `dkArea` column if missing (calculated from `municipalityCode`)

### Phase 2: Join Real Data (1-5 min)
1. **Loads real consumption** from `/historical/{YEAR}/consumption/{MM}.avro`
   - Joins on: `timestamp` + `municipalityCode`
2. **Loads real prices** from `/historical/{YEAR}/price.avro`
   - Joins on: `timestamp` + `dkArea`
3. **Skips files** if no real data available (will retry next cycle)

### Phase 3: Calculate Precision (<1 min)
**Formula:**
```python
precision = 100 × (1 - |predicted - actual| / actual)
precision = max(0, precision)  # Clip at 0%
```

**Columns added:**
- `consumptionPrecision`: Consumption prediction accuracy (0-100%)
- `pricePrecision`: Price prediction accuracy (0-100%)

**Special cases:**
- If `actual == 0` or `null` → precision = 0.0

### Phase 4: Write Output (1-5 min)
1. **Overwrites** prediction files with precision columns added
2. **Generates summary** per municipality-year-month:
   - Min, Max, Avg, StdDev for consumption precision
   - Min, Max, Avg, StdDev for price precision
3. **Writes summary** to `/analytics/predictions_precision_{YEAR}.parquet`

### State Tracking
**File:** `/utils/precision_state.txt` in HDFS

Stores last processed timestamp to enable incremental processing.

## Output Schema

### Enriched Predictions
`/historical/archives/{YEAR}/{MM}/analytics/predictions_*.parquet`:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | timestamp | Hour of prediction |
| `municipalityCode` | int | Municipality (101-813) |
| `dkArea` | int | DK region (1=West, 2=East) |
| `consumptionkWh` | double | Predicted consumption |
| `mean_temp` | double | Temperature (°C) |
| `mean_radiation` | double | Solar radiation (W/m²) |
| `mean_wind_speed` | double | Wind speed (m/s) |
| `productionkWh` | double | Green energy production |
| `price` | double | Predicted price (EUR/MWh) |
| `realConsumptionKwh` | double | Actual consumption |
| `realPrice_EUR_MWh` | double | Actual price |
| **`consumptionPrecision`** | **double** | **Consumption accuracy (%)** |
| **`pricePrecision`** | **double** | **Price accuracy (%)** |

### Summary Statistics
`/analytics/predictions_precision_{YEAR}.parquet`:

| Column | Type | Description |
|--------|------|-------------|
| `municipalityCode` | int | Municipality |
| `dkArea` | int | DK region |
| `year` | int | Year |
| `month` | int | Month |
| `minConsumptionPrecision` | double | Min consumption accuracy |
| `maxConsumptionPrecision` | double | Max consumption accuracy |
| `avgConsumptionPrecision` | double | Avg consumption accuracy |
| `stdConsumptionPrecision` | double | StdDev consumption accuracy |
| `minPricePrecision` | double | Min price accuracy |
| `maxPricePrecision` | double | Max price accuracy |
| `avgPricePrecision` | double | Avg price accuracy |
| `stdPricePrecision` | double | StdDev price accuracy |

## Quick Start

### Prerequisites

```bash
# Verify prediction files exist
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/archives/2025/12/analytics/

# Verify real data exists
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/2024/consumption/

kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/2024/price.avro
```

### Deploy

```bash
# 1. Build Docker image
docker build --platform linux/amd64 \
  -t registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/batch-precision-calculation:latest .

# 2. Push to registry
docker push registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/batch-precision-calculation:latest

# 3. Deploy to Kubernetes
kubectl apply -f batch-precision-deployment.yaml

# 4. Monitor logs
kubectl logs -f deployment/batch-precision-calculation -n bd-bd-gr-05
```

## Monitoring

### Check Logs

```bash
# Follow logs in real-time
kubectl logs -f deployment/batch-precision-calculation -n bd-bd-gr-05

# View recent logs
kubectl logs --tail=100 deployment/batch-precision-calculation -n bd-bd-gr-05
```

**Expected Log Output:**
```
============================================================
Starting processing run at 2025-12-15 14:00:00
============================================================

📅 No previous state found - this is the first run

============================================================
Processing Prediction Precision Data
Processing all available data (first run)
============================================================

📖 [Step 1/5] Reading prediction data from HDFS...
  Found 1 year-month combinations to process
    Found 3 prediction file(s) for 2025-12
✓ Loaded 7,946 prediction records in 5.1s

📖 [Step 2/5] Loading real data for 1 year-month combinations...
  [1/1] Processing 2025-12...
  🔧 [Step 3/5] Calculating precision for 2025-12...
  Adding dkArea column based on municipalityCode...
  Joining with real consumption data...
  Joining with real price data...
  Calculating consumption precision...
  Calculating price precision...
  ✓ Precision calculation complete
✓ Real data loaded and precision calculated in 0.4s

🔧 Combining results from all year-month partitions...
✓ Combined 7,946 records with precision data

💾 [Step 4/5] Writing precision data to HDFS...
  Caching precision data to break file dependencies...
  Writing precision data for 1 year-month combinations...
    [1/1] Writing 2025-12: 7,946 records...
      ✓ Completed in 3.2s

📊 [Step 5/5] Writing precision summary statistics...
  Writing summary for 1 year(s)...
    [1/1] Processing year 2025...
      ✓ Summary written → hdfs://namenode-g5:9000/analytics/predictions_precision_2025.parquet

✅ Processing completed successfully in 0m 42s

😴 Sleeping until next run at 2025-12-15 20:00:00
   (waiting 6 hours...)
```

## Verification

### Check Enriched Files

```bash
# List prediction files (should have precision columns)
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/archives/2025/12/analytics/

# Sample data
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- spark-shell \
  --packages org.apache.spark:spark-avro_2.12:3.4.1

# In Spark shell:
val df = spark.read.parquet("hdfs://namenode-g5:9000/historical/archives/2025/12/analytics")
df.printSchema()  // Should show consumptionPrecision and pricePrecision
df.select("municipalityCode", "consumptionPrecision", "pricePrecision").show(10)
```

### Check Summary Files

```bash
# List summary files
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /analytics/predictions_precision_*.parquet

# Sample summary
val summary = spark.read.parquet("hdfs://namenode-g5:9000/analytics/predictions_precision_2025.parquet")
summary.printSchema()
summary.orderBy("municipalityCode", "month").show(20)
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HDFS_NAMENODE` | `hdfs://namenode-g5:9000` | HDFS namenode URI |
| `SLEEP_INTERVAL_HOURS` | `6` | Hours between processing runs |

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

## Troubleshooting

### Issue: No Prediction Data to Process

**Symptoms:**
```
ℹ️  No new prediction data to process
```

**Solutions:**
- Verify ML prediction job has run and generated prediction files
- Check if predictions exist in archives directory
- Reset state file to reprocess all data:
```bash
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -rm /utils/precision_state.txt
```

### Issue: Real Data Missing

**Symptoms:**
```
⚠️  No real data available for any prediction records, skipping write
```

**Solutions:**
- Verify consumption enrichment job has run
- Verify price data exists in HDFS
- Check that prediction timestamps match available real data timestamps

### Issue: File Not Found Error

**Symptoms:**
```
SparkFileNotFoundException: File does not exist: .../part-00000-xxx.parquet
```

**Cause:** Files changed during processing (race condition)

**Fix:** Already implemented with `.cache()` to materialize data in memory before overwriting.

## Files

```
precision-calculator/
├── src/
│   ├── data_reader.py              # Reads predictions & real data
│   ├── precision_calculator.py     # Calculates precision metrics
│   └── data_writer.py              # Writes enriched data & summaries
├── main.py                         # Orchestrates job (6h loop)
├── Dockerfile
├── batch-precision-deployment.yaml
└── requirements.txt
```

## Key Features

- ✅ **Incremental Processing** - Only processes new prediction files
- ✅ **Automatic dkArea** - Calculates from municipalityCode if missing
- ✅ **Selective File Reading** - Only reads `predictions_*.parquet` files
- ✅ **Graceful Skipping** - Skips files when real data unavailable
- ✅ **Summary Statistics** - Aggregated metrics per municipality-month
- ✅ **State Tracking** - Timestamp-based incremental processing
- ✅ **Race Condition Safe** - Caches data before overwriting files

---

**Namespace:** `bd-bd-gr-05`  
**HDFS Namenode:** `hdfs://namenode-g5:9000`  
**Runtime:** Continuous (6h sleep cycle)  
**Processing Time:** ~5-15 minutes per run