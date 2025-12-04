# Batch Enrichment Job - Consumption Data

This batch enrichment job processes historical consumption data from HDFS, enriches it with `dkArea` based on municipality codes, and writes the enriched data back to HDFS in AVRO format.

## Overview

### Input
- **Source**: `/raw/initial-load/consumption/*.csv` in HDFS
- **Data**: Consumption records with columns:
  - ConsumptionkWh
  - HeatingCategory
  - HousingCategory
  - Municipality
  - MunicipalityCode
  - RegionName
  - TimeDK
  - TimeUTC

### Processing
1. Reads all consumption CSV files from HDFS
2. Renames columns to camelCase (e.g., `MunicipalityCode` → `municipalityCode`)
3. Enriches data by adding `dkArea` column:
   - Looks up municipality code in `municipality_codes_to_coordinates.csv`
   - Retrieves coordinates (latitude, longitude)
   - Calculates `dkArea`: `1` if `longitude < 11`, else `2`
4. Partitions data by year and month

### Output
- **Destination**: `/historical/{YEAR}/consumption/{MONTH}.avro` in HDFS
- **Format**: AVRO
- **Structure**: One file per year-month combination
- **Schema**: All original columns (in camelCase) + `dkArea`

## Files

1. **batch_enrichment_consumption.py** - Main Spark job script
2. **Dockerfile** - Container image definition
3. **batch-enrichment-consumption-deployment.yaml** - Kubernetes deployment
4. **requirements.txt** - Python dependencies
5. **data/municipality_codes_to_coordinates.csv** - Municipality lookup data 

Expected CSV format for **data/municipality_codes_to_coordinates.csv**:
```csv
code,name,latitude,longitude
766,Hedensted,55.793439,9.740474
561,Esbjerg,55.419682,8.683988
849,Jammerbugt,57.074995,9.785064
```


## Build and Deploy

### 1. Build Docker Image

```bash
# Make sure you have the municipality CSV file in place
ls data/municipality_codes_to_coordinates.csv

# Build the image
docker build --platform linux/amd64 -t registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/batch-enrichment-consumption:latest .
# Push to GitLab registry
docker push registry.gitlab.sdu.dk/the-european-avengers/bigdataproject/batch-enrichment-consumption:latest
```

### 2. Deploy to Kubernetes

```bash
# Apply the deployment
kubectl apply -f batch-enrichment-consumption-deployment.yaml

# Check pod status
kubectl get pods -n bd-bd-gr-05 | grep batch-enrichment-consumption

# View logs
kubectl logs -f deployment/batch-enrichment-consumption -n bd-bd-gr-05
```

## Monitoring

### Check Spark UI
The Spark UI is available at port 4040:
```bash
# Port forward to access locally
kubectl port-forward -n bd-bd-gr-05 deployment/batch-enrichment-consumption 4040:4040
```
Then open: http://localhost:4040

### View Logs
```bash
# Follow logs in real-time
kubectl logs -f deployment/batch-enrichment-consumption -n bd-bd-gr-05

# View last 100 lines
kubectl logs --tail=100 deployment/batch-enrichment-consumption -n bd-bd-gr-05
```

## Verify Output

### Check HDFS Output
```bash
# Get a shell in the namenode
kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- /bin/bash

# List output directories
hdfs dfs -ls /historical/
hdfs dfs -ls /historical/2022/consumption/
hdfs dfs -ls /historical/2023/consumption/
hdfs dfs -ls /historical/2024/consumption/
hdfs dfs -ls /historical/2025/consumption/

# Check file sizes
hdfs dfs -du -h /historical/2024/consumption/

# Sample one file (AVRO format)
hdfs dfs -ls /historical/2024/01/consumption/
```

## Output Schema

The enriched AVRO files contain:

```
- consumptionKwh (double)
- heatingCategory (string)
- housingCategory (string)
- municipality (string)
- municipalityCode (int)
- regionName (string)
- timeDK (string)
- timeUTC (timestamp)
- dkArea (int)
```

## Troubleshooting

### Job Fails to Start
```bash
# Check pod events
kubectl describe pod -l app=batch-enrichment-consumption -n bd-bd-gr-05

# Check if municipality CSV is mounted
kubectl exec deployment/batch-enrichment-consumption -n bd-bd-gr-05 -- ls -lah /home/sparkuser/data/
```

### HDFS Connection Issues
```bash
# Check if namenode is accessible
kubectl exec deployment/batch-enrichment-consumption -n bd-bd-gr-05 -- ping namenode-g5

# Check HDFS namenode web UI
kubectl port-forward -n bd-bd-gr-05 service/namenode-g5 9870:9870
```
Then open: http://localhost:9870

### Out of Memory
If the job fails with OOM errors, increase resources in the deployment YAML:
```yaml
resources:
  requests:
    cpu: "4"
    memory: "8Gi"
  limits:
    cpu: "8"
    memory: "16Gi"
```

## Performance Notes

- **Processing Time**: Depends on data volume (~38 CSV files, approx. 1.9GB total)
- **Resource Usage**: 
  - CPU: 2-4 cores recommended
  - Memory: 4-8GB recommended
  - Scales with data size
- **Parallelism**: Configured with 8 shuffle partitions (adjustable in code)

## Cleanup

To remove the deployment:
```bash
kubectl delete deployment batch-enrichment-consumption -n bd-bd-gr-05
kubectl delete service batch-enrichment-consumption -n bd-bd-gr-05
```

## Differences from Weather Enrichment

1. **Input Data**: Consumption CSV files instead of weather forecast data
2. **Municipality Lookup**: Direct lookup by `municipalityCode` (no nearest-neighbor search needed)
3. **Column Naming**: Renames all columns to camelCase
4. **Output Structure**: Single weather type (consumption) vs. three weather types (wind, temp, sun)
5. **DK Area Calculation**: Based on municipality coordinates from lookup table
ions, contact the data engineering team.