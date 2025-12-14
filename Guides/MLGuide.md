
# Energy Consumption & Price ML Predictor

Spark-based ML job that predicts hourly energy consumption and electricity prices for all 98 Danish municipalities using XGBoost models.

## What It Does

Reads historical data from HDFS, trains prediction models, generates forecasts, and writes results back to HDFS:

- **Predicts** hourly energy consumption per municipality (next 1-30 days)
- **Predicts** electricity prices for DK1 and DK2 regions
- **Calculates** green energy production (wind + solar)
- **Writes** predictions to `/analytics/predictions.parquet` (main file)
- **Archives** predictions to `/historical/archives/{YEAR}/{MONTH}/analytics/`

## Data Flow

```
Input (HDFS)                        Processing                          Output (HDFS)
                                                                       
/historical/{YEAR}/          →      Spark Job                    →     /analytics/
├── consumption/                    ├── Train Models                   └── predictions.parquet
│   └── {MM}.avro                   ├── Generate Predictions               (overwritten each run)
├── weather-temp/                   └── Calculate Production
│   └── {MM}.avro                                                      /historical/archives/
├── weather-sun/                                                       └── {YEAR}/{MM}/analytics/
│   └── {MM}.avro                                                          └── predictions_{timestamp}.parquet
├── weather-wind/                                                             (archived by month)
│   └── {MM}.avro
├── production/
│   └── {MM}.avro
├── price.avro
└── ...

/live/forecast/              →      Forecast Input              →      Used for predictions
├── weather-temp/
├── weather-sun/
└── weather-wind/
```

## How It Works

### Phase 1: Data Loading (10-15 min)
1. **Loads historical data** from HDFS (3 years by default)
   - Consumption, weather (temp, sun, wind), production, prices
2. **Validates date ranges** to ensure complete data
3. **Persists to memory** to prevent file invalidation issues

### Phase 2: Model Training (30-60 min)
1. **Feature Engineering:**
   - Time features: hour, day of week, month, cyclical encoding
   - Weather interactions: temp × sunlight, cold & dark flags
   - Lag features: same hour last year, same day last year average

2. **Consumption Models (XGBoost):**
   - **Per-municipality models** for 98+ municipalities
   - **Global fallback model** for municipalities with insufficient data (<1000 records)
   - Year-over-year trend adjustment per municipality

3. **Price Models (Ensemble):**
   - Separate models for DK1 and DK2
   - Tests XGBoost, RandomForest, GradientBoosting
   - Selects best model per region based on MAE
   - Features: time patterns + supply/demand balance
   - Historical price constraints to prevent unrealistic predictions

### Phase 3: Prediction Generation (5-10 min)
1. **Loads forecast weather** from `/live/forecast/` (auto-detects available dates)
2. **Predicts consumption** for each municipality using trained models
3. **Calculates production:**
   - Solar: `capacity_kw × (radiation / 1000) × 0.15 × 1 hour`
   - Wind: `capacity_kw × capacity_factor(wind_speed) × 1 hour`
4. **Predicts prices** for DK1 and DK2 based on consumption/production balance
5. **Merges results** into single dataset

### Phase 4: Writing Output (<1 min)
1. **Overwrites** `/analytics/predictions.parquet` (main file, always current)
2. **Archives** to `/historical/archives/{YEAR}/{MONTH}/analytics/predictions_{timestamp}.parquet`

## ML Models

### Consumption Prediction (XGBoost)
```python
XGBRegressor(
    n_estimators=200,
    learning_rate=0.05,
    max_depth=6,
    min_child_weight=3,
    subsample=0.8,
    colsample_bytree=0.8
)
```
**Accuracy:** MAE ~50-150 kWh, MAPE ~8-15% (varies by municipality)

### Price Prediction (Ensemble)
Tests multiple algorithms (XGBoost, RandomForest, GradientBoosting) and selects the best per region.

**Key Feature:** Historical price constraints prevent unrealistic predictions by bounding forecasts within previous year's same-month range (±20% buffer).

## Output Schema

`/analytics/predictions.parquet` contains:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | timestamp | Hour of prediction |
| `municipalityCode` | int | Municipality (101-813) |
| `consumptionkWh` | double | Predicted consumption |
| `mean_temp` | double | Temperature (°C) |
| `mean_radiation` | double | Solar radiation (W/m²) |
| `mean_wind_speed` | double | Wind speed (m/s) |
| `productionkWh` | double | Green energy production |
| `price` | double | Electricity price (EUR/MWh) |

## Quick Start

### Prerequisites

```bash
# Verify HDFS data exists
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/2024/consumption/

kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /live/forecast/weather-temp/

# Verify capacity data exists
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /utils/solar_panels.csv
```

### Deploy

```bash
# 1. Build Docker image
docker build -t registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/energy-ml-predictor:latest .

# 2. Push to registry
docker push registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/energy-ml-predictor:latest

# 3. Deploy to Kubernetes
kubectl apply -f k8s/deployment.yaml

# 4. Monitor logs
kubectl logs -f $(kubectl get pods -n bd-bd-gr-05 -l job-name=energy-ml-predictor -o jsonpath='{.items[0].metadata.name}') -n bd-bd-gr-05
```

### Expected Runtime
- **Total:** ~45-90 minutes (depending on data volume)
- Loading: 10-15 min
- Training: 30-60 min
- Prediction: 5-10 min
- Writing: <1 min

## Configuration

### Training Years
Adjust in `k8s/deployment.yaml`:
```yaml
args:
  - --training-years 3  # Default is 3
```

### Specific Dates (Optional)
To predict specific dates instead of all forecast data:
```yaml
args:
  - --training-years 3
  - --days 2024-12-01,2024-12-02,2024-12-03
```

### Resources
```yaml
resources:
  requests:
    memory: "4Gi"
    cpu: "1"
  limits:
    memory: "8Gi"
    cpu: "2"
```
Increase for larger datasets or more municipalities.

## Monitoring

### View Logs
```bash
# Get pod name
POD_NAME=$(kubectl get pods -n bd-bd-gr-05 -l job-name=energy-ml-predictor -o jsonpath='{.items[0].metadata.name}')

# Follow logs (only new messages)
kubectl logs -f $POD_NAME -n bd-bd-gr-05 --tail=0

# View recent logs
kubectl logs $POD_NAME -n bd-bd-gr-05 --tail=50
```

### Check Progress
Look for these key messages:
```
STEP 1: Loading Training Data
STEP 2: Training Consumption Model
  Training municipality 101 (1/98)
  ...
STEP 3: Training Price Models
STEP 4: Loading Forecast Weather Data
STEP 5: Generating Consumption Predictions
STEP 6: Calculating Production from Forecast
STEP 7: Generating Price Predictions
STEP 8: Merging Predictions
STEP 9: Writing Predictions
✓ SPARK JOB COMPLETED SUCCESSFULLY
```

## Verification

### Check Output Files
```bash
# Main predictions file
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /analytics/predictions.parquet

# Archive (latest)
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /historical/archives/2025/12/analytics/

# File size
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -du -h /analytics/predictions.parquet
```

### Sample Data
```bash
# Start Spark shell
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- spark-shell \
  --packages org.apache.spark:spark-avro_2.12:3.4.1

# In Spark shell:
val df = spark.read.parquet("hdfs://namenode-g5:9000/analytics/predictions.parquet")
df.printSchema()
df.count()
df.show(10, false)

// Check date range
df.selectExpr("min(timestamp)", "max(timestamp)").show()

// Summary statistics
df.describe("consumptionkWh", "productionkWh", "price").show()
```

## Troubleshooting

### Job Fails with File Not Found
**Cause:** Files changed during training (race condition)

**Fix:** Already implemented with `.persist()` in code to cache data in memory.

### Out of Memory
```bash
# Increase memory in k8s/deployment.yaml:
limits:
  memory: "12Gi"  # Increase from 8Gi
```

### No Predictions Generated
```bash
# Check if forecast data exists
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- \
  hdfs dfs -ls /live/forecast/

# Check producer logs
kubectl logs deployment/kafka-producer-1-g5 -n bd-bd-gr-05
```

### Price Predictions Are Unrealistic
Price model includes historical constraints by default. If needed, adjust buffer in `src/models/price_predictor.py`:
```python
buffer = 0.2  # 20% range expansion
```

## Files

```
ml-consumption-predictor/
├── src/
│   ├── config/            # Settings (local/kubernetes modes)
│   ├── data/              # Data loaders/writers (HDFS/CSV)
│   ├── features/          # Feature engineering
│   ├── models/            # ML trainers/predictors
│   ├── production/        # Green energy calculator
│   ├── utils/             # Spark utilities
│   └── main.py            # Job orchestration
├── k8s/
│   └── deployment.yaml    # Kubernetes Job definition
├── Dockerfile             # Container image
└── requirements.txt       # Python dependencies
```

## Local Testing (Optional)

The code supports local mode with CSV files for development/testing. See `README.md` for details.

## Key Features

- ✅ **Distributed Processing** - Spark-based for scalability
- ✅ **Per-Municipality Models** - Customized predictions for each municipality
- ✅ **Automatic Fallback** - Global model for municipalities with insufficient data
- ✅ **Trend Adjustment** - Year-over-year growth/decline per municipality
- ✅ **Price Constraints** - Historical bounds prevent unrealistic predictions
- ✅ **Data Validation** - Handles partial/missing data gracefully
- ✅ **File Rotation** - Main file (latest) + monthly archives
- ✅ **Persistent Caching** - Prevents file invalidation issues

---

**Namespace:** `bd-bd-gr-05`  
**HDFS Namenode:** `hdfs://namenode-g5:9000`  
**Runtime:** ~45-90 minutes (full pipeline)