# Data Transfer Guide

## Overview
Your data collection produces **3 different data types**, each requiring its own Hive database and table.

## Data Types & Structure

### 1️⃣ **Wind Data** (`*_dmi_wind.csv`)
- **Files**: `2021_dmi_wind.csv`, `2022_dmi_wind.csv`, etc.
- **Database**: `dmi_wind`
- **Table**: `wind_raw_data`
- **Schema**:
  - `timeObserved` STRING
  - `stationId` STRING
  - `stationName` STRING
  - `mean_wind_speed` DOUBLE
- **HDFS Location**: `hdfs://namenode-g5:9000/raw/initial/weather-wind`

### 2️⃣ **Sun Data** (`*_dmi_sun.csv`)
- **Files**: `2020_dmi_sun.csv`, `2021_dmi_sun.csv`, etc.
- **Database**: `dmi_sun`
- **Table**: `sun_raw_data`
- **Schema**:
  - `timeObserved` STRING
  - `stationId` STRING
  - `stationName` STRING
  - `mean_radiation` DOUBLE
- **HDFS Location**: `hdfs://namenode-g5:9000/raw/initial/weather-sun`

### 3️⃣ **Heating Consumption Data** (`heating_consumption_*.csv`)
- **Files**: `heating_consumption_2022_09.csv`, `heating_consumption_2023_01.csv`, etc.
- **Database**: `energy_heating`
- **Table**: `heating_consumption_raw`
- **Schema**:
  - `ConsumptionkWh` DOUBLE
  - `HeatingCategory` STRING
  - `HousingCategory` STRING
  - `Municipality` STRING
  - `MunicipalityCode` INT
  - `RegionName` STRING
  - `TimeDK` STRING
  - `TimeUTC` STRING
- **HDFS Location**: `hdfs://namenode-g5:9000/raw/initial/heating-consumption`

## Usage

### Run the automated transfer script:
```bash
cd /Users/arejula11/Documents/Master/BigData/BigDataProject/kubernetes/hive/initial-data
./data_transfer_all.sh
```

This script will:
1. ✅ Verify Hive deployment is running
2. ✅ List all CSV files in the shared volume
3. ✅ Create 3 separate databases and tables
4. ✅ Load all matching CSV files into each table
5. ✅ Verify data counts
6. ✅ Show summary with HDFS locations

## Querying the Data

### Wind Data:
```sql
USE dmi_wind;
SELECT * FROM wind_raw_data LIMIT 10;
SELECT COUNT(*) FROM wind_raw_data;
```

### Radiation Data:
```sql
USE dmi_radiation;
SELECT * FROM radiation_raw_data LIMIT 10;
SELECT COUNT(*) FROM radiation_raw_data;
```

### Heating Data:
```sql
USE energy_heating;
SELECT * FROM heating_consumption_raw LIMIT 10;
SELECT COUNT(*) FROM heating_consumption_raw;
```

## Files in Shared Directory

Based on your listing, you have:
- **5 wind files** (2021-2024)
- **5 radiation files** (2020-2024)
- **~40 heating consumption files** (monthly from 2022-09 to 2025-11)
- **1 combined heating file** (all data merged)

All files will be automatically detected and loaded by the script!

## Troubleshooting

### Check shared volume contents:
```bash
kubectl exec -n bd-bd-gr-05 deployment/hive-server -c hive -- \
  ls -lh /shared-data-for-hive/
```

### Check HDFS contents:
```bash
kubectl exec -n bd-bd-gr-05 deployment/hive-server -c hive -- \
  hdfs dfs -ls /raw/initial/
```

### Manual Hive access:
```bash
kubectl exec -it -n bd-bd-gr-05 deployment/hive-server -c hive -- hive
```
