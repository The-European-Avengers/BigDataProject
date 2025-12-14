# Energy Consumption ML Predictor - Spark Job

Production-ready **Apache Spark job** for predicting hourly energy consumption per municipality in Denmark using XGBoost with year-over-year trend adjustment.

## Features

- ✅ **True Spark Job** with distributed processing
- ✅ **Dual Mode**: Local CSV or Kubernetes Avro/Parquet
- ✅ **Smart Forecast Loading**: Automatic fallback from archived forecast to historical weather
- ✅ **Streaming Support**: Handles forecast batches from Spark Streaming
- ✅ **Data Validation**: Handles partial/missing data gracefully
- ✅ **HDFS Integration**: Native HDFS paths (hdfs://namenode-g5:9000)

## Project Structure

```
ml-consumption-predictor/
├── src/                      # Source code
├── k8s/                      # Kubernetes manifests
├── data/                     # Local data (development)
│   ├── csvs/
│   │   ├── consumption/
│   │   │   ├── 2020.csv
│   │   │   ├── 2021.csv
│   │   │   └── ...
│   │   ├── weather/
│   │   │   ├── temp_2020.csv
│   │   │   ├── sun_2020.csv
│   │   │   ├── wind_2020.csv
│   │   │   └── ...
│   │   └── forecast/
│   │       ├── temp.csv
│   │       ├── sun.csv
│   │       └── wind.csv
│   └── analytics/            # Output predictions
│       ├── 2024-12-01.csv
│       ├── 2024-12-02.csv
│       └── ...
├── deploy.sh
├── Dockerfile
├── requirements.txt
└── README.md
```

## Local Mode (Development)

### Data Structure
```
ml-consumption-predictor/
└── data/
    ├── csvs/
    │   ├── consumption/
    │   │   ├── 2020.csv
    │   │   ├── 2021.csv
    │   │   ├── 2022.csv
    │   │   ├── 2023.csv
    │   │   └── 2024.csv
    │   │
    │   ├── weather/
    │   │   ├── temp_2020.csv
    │   │   ├── temp_2021.csv
    │   │   ├── sun_2020.csv
    │   │   ├── sun_2021.csv
    │   │   ├── wind_2020.csv
    │   │   └── wind_2021.csv
    │   │
    │   └── forecast/
    │       ├── temp.csv
    │       ├── sun.csv
    │       └── wind.csv
    │
    └── analytics/              # OUTPUT
        ├── 2024-12-01.csv
        ├── 2024-12-02.csv
        └── 2024-12-03.csv
```

### Path Resolution
The application automatically finds the project root and constructs paths:
- Consumption: `{project_root}/data/csvs/consumption/{year}.csv`
- Weather: `{project_root}/data/csvs/weather/{type}_{year}.csv`
- Forecast: `{project_root}/data/csvs/forecast/{type}.csv`
- Analytics: `{project_root}/data/analytics/{year}-{month}-{day}.csv`

Directories are created automatically if they don't exist.

## Kubernetes Mode (Production)

### HDFS Structure
```
hdfs://namenode-g5:9000/
├── analytics/                           # Current predictions
│   ├── consumption_2025-12-08.parquet
│   ├── consumption_2025-12-09.parquet
│   └── consumption_2025-12-10.parquet
│
├── historical/
│   ├── <year>/
│   │   ├── consumption/<month>.avro/
│   │   ├── weather-temp/<month>.avro/
│   │   ├── weather-sun/<month>.avro/
│   │   ├── weather-wind/<month>.avro/
│   │   │
│   │   ├── forecast-temp/<month>/
│   │   │   └── <day-HH-MM>_batch-*_<uuid>/
│   │   ├── forecast-sun/<month>/
│   │   └── forecast-wind/<month>/
│   │
│   └── archives/<year>/<month>/analytics/
│       └── consumption_<uuid>.parquet
│
└── live/forecast/
    ├── weather-temp/part-*.avro
    ├── weather-sun/part-*.avro
    └── weather-wind/part-*.avro
```

## Usage

### Local Mode

```bash
# Navigate to project root
cd ml-consumption-predictor

# Run with default settings (all forecast days, 4 training years)
python -m src.main --mode local

# Specific days
python -m src.main \
  --mode local \
  --days 2024-12-01,2024-12-02,2024-12-03 \
  --training-years 3

# With spark-submit (recommended for local development)
spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-avro_2.12:3.5.0 \
  src/main.py \
  --mode local \
  --training-years 2
```

**Expected Output:**
```
Local data root: /path/to/ml-consumption-predictor/data
Consumption path: /path/to/ml-consumption-predictor/data/csvs/consumption
Weather path: /path/to/ml-consumption-predictor/data/csvs/weather
Forecast path: /path/to/ml-consumption-predictor/data/csvs/forecast
Analytics path: /path/to/ml-consumption-predictor/data/analytics

Loading consumption data for years: [2020, 2021, 2022, 2023]
Loaded /path/to/.../data/csvs/consumption/2020.csv: 8760 records
...
✓ Written 2,400 predictions to /path/to/.../data/analytics/2024-12-01.csv
```

### Kubernetes Mode

```bash
# Default (all live forecast days)
python -m src.main --training-years 3

# Specific days with archived forecast fallback
python -m src.main \
  --days 2025-12-05,2025-12-06,2025-12-07 \
  --training-years 3

# Deploy to Kubernetes
./deploy.sh all
```

## Command Line Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--days` | str | None | Comma-separated dates (YYYY-MM-DD). If omitted, uses all forecast days |
| `--training-years` | int | 4 | Number of years for training data |
| `--mode` | str | kubernetes | `local` or `kubernetes` |

## Smart Forecast Loading

### Local Mode
Always uses files in `data/csvs/forecast/`:
- `temp.csv`, `sun.csv`, `wind.csv`
- Filters to specific dates if `--days` provided

### Kubernetes Mode

**No specific dates** (default):
- Uses `/live/forecast/weather-*/part-*.avro`

**With specific dates**:
1. **Try**: `/historical/<year>/forecast-*/<month>/<day-HH-MM>_batch-*_<uuid>/`
2. **Fallback**: `/historical/<year>/weather-*/<month>.avro/`

## Setup for Local Development

### 1. Create Directory Structure
```bash
cd ml-consumption-predictor

# Directories are auto-created, but you can prepare them:
mkdir -p data/csvs/consumption
mkdir -p data/csvs/weather
mkdir -p data/csvs/forecast
mkdir -p data/analytics
```

### 2. Add Sample Data

**Consumption** (`data/csvs/consumption/2023.csv`):
```csv
consumptionKwh,Municipality,MunicipalityCode,RegionName,TimeDK,TimeUTC
1500.5,København,101,Region Hovedstaden,2023-01-01T00:00:00,2023-01-01T00:00:00
1450.2,København,101,Region Hovedstaden,2023-01-01T01:00:00,2023-01-01T01:00:00
...
```

**Weather** (`data/csvs/weather/temp_2023.csv`):
```csv
lat,lon,parameter,timestamp,value,dkArea,municipalityCode
55.676,12.568,temperature-2m,2023-01-01T00:00:00,5.2,1,101
55.676,12.568,temperature-2m,2023-01-01T01:00:00,5.0,1,101
...
```

**Forecast** (`data/csvs/forecast/temp.csv`):
```csv
lat,lon,parameter,timestamp,value,dkArea,municipalityCode
55.676,12.568,temperature-2m,2024-12-01T00:00:00,3.5,1,101
55.676,12.568,temperature-2m,2024-12-01T01:00:00,3.2,1,101
...
```

### 3. Run
```bash
python -m src.main --mode local --training-years 2
```

## Output Schema

### Local Mode CSV
```
data/analytics/2024-12-01.csv
├── timestamp (YYYY-MM-DDTHH:MM:SS)
├── municipalityCode (integer)
└── consumptionkWh (float)
```

### Kubernetes Mode Parquet
```
/analytics/consumption_2024-12-01.parquet
├── timestamp (TIMESTAMP)
├── municipalityCode (INT)
└── consumptionkWh (DOUBLE)
```

## Examples

### Example 1: Local Development with Sample Data
```bash
# Prepare sample data
cd ml-consumption-predictor
python scripts/generate_sample_data.py  # If you have a generator

# Run prediction
python -m src.main --mode local --training-years 2

# Check output
ls -lh data/analytics/
cat data/analytics/2024-12-01.csv
```

### Example 2: Local with Specific Days
```bash
python -m src.main \
  --mode local \
  --days 2024-12-01,2024-12-05,2024-10 \
  --training-years 3

# Output:
# data/analytics/2024-12-01.csv
# data/analytics/2024-12-05.csv
# data/analytics/2024-12-10.csv
```

### Example 3: Kubernetes Production
```bash
# Deploy
kubectl apply -f k8s/job.yaml

# Monitor
kubectl logs -f energy-ml-predictor-manual-driver

# Check output
hdfs dfs -ls hdfs://namenode-g5:9000/analytics/
```

## Model Details

### XGBoost Configuration
```python
n_estimators=200
learning_rate=0.05
max_depth=6
min_child_weight=3
subsample=0.8
colsample_bytree=0.8
```

### Features
- **Time**: hour, day of week, month, cyclical encoding
- **Weather**: temperature, sunlight, interactions
- **Lags**: same hour last year, same day average last year
- **Trend**: year-over-year multiplier per municipality

## Troubleshooting

### Local Mode Issues

#### File Not Found
**Symptoms**: `FileNotFoundError: Forecast file not found`

**Solution**: Check paths:
```bash
cd ml-consumption-predictor
ls -la data/csvs/forecast/
ls -la data/csvs/consumption/
ls -la data/csvs/weather/
```

Ensure files exist with correct names:
- `data/csvs/consumption/2023.csv`
- `data/csvs/weather/temp_2023.csv`
- `data/csvs/forecast/temp.csv`

#### Wrong Working Directory
**Symptoms**: Paths not resolving correctly

**Solution**: Always run from project root:
```bash
cd ml-consumption-predictor
python -m src.main --mode local
```

#### Permission Denied
**Symptoms**: Cannot write to analytics folder

**Solution**: Check permissions:
```bash
chmod -R u+w data/analytics/
```

### Kubernetes Mode Issues

#### HDFS Connection
**Symptoms**: Cannot connect to namenode

**Solution**: 
```bash
# Test HDFS connectivity
kubectl exec -it <driver-pod> -- hdfs dfs -ls hdfs://namenode-g5:9000/
```

## Performance

| Metric | Local | Kubernetes |
|--------|-------|------------|
| Training (3 years, 10 munis) | 2-3 min | 1-2 min |
| Prediction (5 days) | 30-60 sec | 15-30 sec |
| Memory | 4-8 GB | 4 GB/executor |

## Development Workflow

1. **Prepare local data** in `data/csvs/`
2. **Run locally** with `--mode local`
3. **Validate output** in `data/analytics/`
4. **Test with Kubernetes mode** on small dataset
5. **Deploy to production**

## Environment Variables

### Local Mode
No environment variables needed. Paths are auto-detected from project structure.

### Kubernetes Mode
Set in `k8s/configmap.yaml`:
```yaml
DATA_BASE_PATH: "hdfs://namenode-g5:9000"
DEPLOYMENT_MODE: "kubernetes"
```

## Kubernetes Deployment

```bash
# 1. Build and push image
docker build -t your-registry/energy-ml-predictor:latest .
docker push your-registry/energy-ml-predictor:latest

# 2. Deploy
./deploy.sh all

# 3. Monitor
kubectl get sparkapplications
kubectl logs -f <app-name>-driver
```