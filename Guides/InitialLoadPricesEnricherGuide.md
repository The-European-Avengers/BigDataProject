# Batch Enrichment Job - Electricity Price Data

This batch job processes historical electricity price data from raw CSV files and enriches them into structured AVRO format.

## Overview

**Input:** `/raw/initial-load/price/*.csv` (10 files: DK1 and DK2 for years 2021-2025)
**Output:** `/historical/{YEAR}/price.avro` (one file per year, merged DK1 + DK2)

### Data Transformation

1. **Extract timestamp** from MTU (UTC) field:
   - Raw: `"31/12/2020 23:00:00 - 01/01/2021 00:00:00"`
   - Extracted: `31/12/2020 23:00:00` (first timestamp)
   - Type: Spark TIMESTAMP

2. **Extract dkArea** from Area field:
   - Raw: `"BZN|DK1"` → `1`
   - Raw: `"BZN|DK2"` → `2`
   - Type: INTEGER

3. **Rename price column:**
   - From: `"Day-ahead Price (EUR/MWh)"`
   - To: `"price_EUR_MWh"`
   - Type: DOUBLE

4. **Merge DK1 and DK2** files per year

5. **Deduplicate** by `(timestamp, dkArea)`

### Final Schema

```
timestamp: timestamp (Spark TIMESTAMP)
dkArea: integer (1 or 2)
price_EUR_MWh: double
```

## Files

- `batch_enrichment_price.py` - Main Spark job script
- `Dockerfile-price` - Docker image definition
- `batch-enrichment-price-deployment.yaml` - Kubernetes deployment

## Deployment Instructions

### 1. Build and Push Docker Image

```bash
# Build the image
docker build --platform linux/amd64 -f Dockerfile-price \
  -t registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/batch-enrichment-price:latest .

# Push to registry
docker push registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/batch-enrichment-price:latest
```

### 2. Deploy to Kubernetes

```bash
# Apply the deployment
kubectl apply -f batch-enrichment-price-deployment.yaml

# Check deployment status
kubectl get deployments -n bd-bd-gr-05 | grep batch-enrichment-price

# Check pod status
kubectl get pods -n bd-bd-gr-05 | grep batch-enrichment-price
```

### 3. Monitor the Job

```bash
# Get pod name
POD_NAME=$(kubectl get pods -n bd-bd-gr-05 -l app=batch-enrichment-price -o jsonpath='{.items[0].metadata.name}')

# Follow logs
kubectl logs -f $POD_NAME -n bd-bd-gr-05

# Or directly
kubectl logs -f deployment/batch-enrichment-price -n bd-bd-gr-05
```

### 4. Verify Output

After the job completes, verify the output in HDFS:

```bash
# Exec into namenode
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- bash

# Check output directories
hdfs dfs -ls /historical/
hdfs dfs -ls /historical/2021/
hdfs dfs -ls /historical/2021/price.avro/

# Check file sizes
hdfs dfs -du -h /historical/2021/price.avro/
hdfs dfs -du -h /historical/2022/price.avro/
hdfs dfs -du -h /historical/2023/price.avro/
hdfs dfs -du -h /historical/2024/price.avro/
hdfs dfs -du -h /historical/2025/price.avro/

# Sample the data (if you have avro-tools)
hdfs dfs -cat /historical/2021/price.avro/part-*.avro | head -n 100
```

## Expected Output Structure

```
/historical/
├── 2021/
│   └── price.avro/
│       ├── part-00000-xxx.avro
│       ├── part-00001-xxx.avro
│       └── ...
├── 2022/
│   └── price.avro/
├── 2023/
│   └── price.avro/
├── 2024/
│   └── price.avro/
└── 2025/
    └── price.avro/
```

Each year's `price.avro` directory contains:
- Merged data from both DK1 and DK2 files
- Deduplicated by (timestamp, dkArea)
- Sorted by timestamp

## Troubleshooting

### Job fails with "File not found"

Check if raw price files exist:
```bash
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -ls /raw/initial-load/price/
```

### Out of Memory errors

Increase memory in deployment YAML:
```yaml
resources:
  requests:
    memory: "8Gi"
  limits:
    memory: "16Gi"
```

### Slow performance

Increase CPU and adjust Spark shuffle partitions in the Python script:
```python
.config("spark.sql.shuffle.partitions", "16")  # Increase from 8
```

## Job Characteristics

- **Processing time:** ~2-5 minutes (depends on data size)
- **Input records:** ~87,600 per year (8,760 hours × 2 areas × ~5 years)
- **Memory usage:** ~2-4 GB
- **Deduplication:** Removes any duplicate (timestamp, dkArea) pairs

## Cleanup

To remove the deployment:
```bash
kubectl delete -f batch-enrichment-price-deployment.yaml
```

To remove output data (use with caution):
```bash
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -rm -r /historical/2021/price.avro
# Repeat for other years as needed
```
