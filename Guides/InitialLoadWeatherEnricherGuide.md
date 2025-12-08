# Initial Load Enricher

Batch Spark job that enriches historical weather data (2020-2024) with geographic metadata and writes partitioned AVRO files to HDFS.

## What It Does

Processes 5 years of historical weather CSV data (~6.5M records) and:
- **Enriches** with `dkArea` (1=West, 2=East Denmark based on longitude < 11)
- **Enriches** with `municipalityCode` (nearest Danish municipality via coordinate lookup)
- **Partitions** by year and month
- **Writes** to HDFS as AVRO format at `/historical/{YEAR}/{TYPE}/{MONTH}.avro`

### Input
```
/raw/initial-load/
├── weather-wind/    (5 CSV files, 2020-2024, ~165 MB)
├── weather-temp/    (5 CSV files, 2020-2024, ~165 MB)
└── weather-sun/     (5 CSV files, 2020-2024, ~65 MB)
```

### Output
```
/historical/
├── 2020/
│   ├── weather-wind/
│   │   ├── 01.avro/  (January 2020 wind data, enriched)
│   │   ├── 02.avro/  (February 2020 wind data, enriched)
│   │   └── ... (12 files)
│   ├── weather-temp/ (12 files)
│   └── weather-sun/  (12 files)
├── 2021/ (36 files)
├── 2022/ (36 files)
├── 2023/ (36 files)
└── 2024/ (36 files)

Total: ~183 AVRO files
```

## How It Works

1. **Reads CSV** from HDFS `/raw/initial-load/`
2. **Parses timestamps** and extracts year/month
3. **Applies enrichment** using pandas UDFs:
   - `dkArea`: Calculated from longitude
   - `municipalityCode`: Nearest-neighbor lookup from 311 Danish municipalities
4. **Writes AVRO** partitioned by year-month to `/historical/`

**Tech Stack:** PySpark 3.4.1, Python 3.10, HDFS, Kubernetes

## Quick Start

### Run the Job

```bash
# Deploy
kubectl apply -f batch-enrichment-deployment.yaml

# Watch progress
kubectl logs -f deployment/batch-enrichment-process -n bd-bd-gr-05
```

### Verify Output

```bash
# Check HDFS structure
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -ls /historical/

# Count files (should be ~183)
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -ls -R /historical/ | grep ".avro" | wc -l
```

## Rerun Job (Test)

```bash
# 1. Delete historical data
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -rm -r /historical

# 2. Verify deletion
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -ls /

# 3. Delete deployment
kubectl delete deployment batch-enrichment-process -n bd-bd-gr-05

# 4. Redeploy
kubectl apply -f batch-enrichment-deployment.yaml

# 5. Watch logs
kubectl logs -f deployment/batch-enrichment-process -n bd-bd-gr-05
```

## Performance

- **Runtime:** 5-15 minutes
- **Memory:** 4-8 GB
- **CPU:** 2-4 cores
- **Input Size:** ~320 MB (6.5M records)
- **Output Size:** ~150-200 MB AVRO (40% compression)

## Files

```
initial-load-enricher/
├── batch_enrichment.py              # Main Spark job (enhanced logging)
├── Dockerfile                       # Docker image with Spark + Avro
├── batch-enrichment-deployment.yaml # Kubernetes Deployment
├── requirements.txt                 # Python dependencies
└── data/
    └── municipality_codes_to_coordinates.csv  # Municipality lookup (311 codes)
```

## Docker Image

**Location:** `registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/batch-enrichment:latest`

**Includes:**
- Python 3.10 + Java 11
- PySpark 3.4.1 with Hadoop 3
- Spark Avro connector JAR
- Municipality lookup data

### Rebuild

```bash
docker build --platform linux/amd64 -t registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/batch-enrichment:latest .

docker push registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/batch-enrichment:latest
```

## Output Schema

Each AVRO file contains:

| Column | Type | Description |
|--------|------|-------------|
| `timeObserved` | timestamp | Observation time |
| `stationId` | string | Weather station ID |
| `stationName` | string | Weather station name |
| `mean_wind_speed` / `mean_temp` / `mean_radiation` | double | Measurement value |
| `lon` | double | Longitude |
| `lat` | double | Latitude |
| **`dkArea`** | **int** | **DK area (1=West, 2=East)** |
| **`municipalityCode`** | **int** | **Municipality code (0-999)** |

## Enrichment Logic

### DK Area
```python
if lon < 11:
    dkArea = 1  # West Denmark
else:
    dkArea = 2  # East Denmark
```

### Municipality Code
```python
# Find nearest municipality using Euclidean distance
coords = municipality_lookup_table  # 311 municipalities
distance = sqrt((lat - muni_lat)² + (lon - muni_lon)²)
municipalityCode = nearest_municipality_code
```

## Troubleshooting

### Job fails to start
```bash
# Check pod status
kubectl get pods -n bd-bd-gr-05 | grep batch-enrichment

# View pod events
kubectl describe pod <pod-name> -n bd-bd-gr-05
```

### No HDFS output
```bash
# Check HDFS permissions
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -ls -d /historical

# Check job logs for errors
kubectl logs <pod-name> -n bd-bd-gr-05 | grep ERROR
```

### Out of memory
```bash
# Increase resources in batch-enrichment-deployment.yaml:
resources:
  requests:
    memory: "8Gi"  # Increase from 4Gi
  limits:
    memory: "16Gi" # Increase from 8Gi
```

## Related Components

- **Kafka Enricher** (streaming): Enriches real-time forecast data → `/raw/forecast/`
- **Batch Enricher** (this job): Enriches historical data → `/historical/`

Both use the same enrichment logic (`dkArea` + `municipalityCode`).

---

**Namespace:** `bd-bd-gr-05`  
**HDFS Namenode:** `hdfs://namenode-g5:9000`  
**Runtime:** ~10-15 minutes for 5 years of data