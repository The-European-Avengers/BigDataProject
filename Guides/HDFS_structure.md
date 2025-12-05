# Creating Directory Structure in HDFS

This guide explains two methods—manual commands or an automation script—to create the complete folder structure in HDFS, accessed via the NameNode pod.

## Prerequisites

Before you begin, make sure:

  * Your HDFS cluster is up and running.
  * The **NameNode** pod (`namenode-g5-0`) and DataNode pods are in the `Running` state:

```bash
kubectl get pods
```

Example output:

```
NAME               READY   STATUS    AGE
namenode-g5-0      1/1     Running   33d
datanode-g5-0      1/1     Running   33d
datanode-g5-1      1/1     Running   33d
datanode-g5-2      1/1     Running   33d
datanode-g5-3      1/1     Running   33d
```

## Manual Steps to Create HDFS Directory Structure

Follow these steps if you prefer to execute commands sequentially.

### Step 1. Access the NameNode Pod

To create directories in HDFS, you need to access the NameNode pod. Use the following command to access the **NameNode** pod directly from your terminal:

```bash
kubectl exec -it namenode-g5-0 -- bash
```

You should now see a shell prompt like:

```
root@namenode-g5-0:/#
```

### Step 2. Create the Base HDFS Directory Structure

Use the `hdfs dfs -mkdir` command to create the top-level folders:

```bash
hdfs dfs -mkdir /raw /historical /live /analytics
```

### Step 3. Creating Yearly and Topic Subdirectories in `/historical`

To organize historical data by **year** and **topic**, run the following script directly in the terminal to create nested folders under `/historical` (2020–2024):

```bash
for year in {2020..2024}; do
  for topic in weather-wind weather-temp weather-sun; do
    hdfs dfs -mkdir -p /historical/$year/$topic
  done
done
```

### Step 4. Creating Topic Subdirectories in `/live`

Create topic subdirectories under the `/live` directory for real-time data:

```bash
hdfs dfs -mkdir /live/weather-temp
hdfs dfs -mkdir /live/weather-wind
hdfs dfs -mkdir /live/weather-sun 
```

### Step 5. Creating Nested Subdirectories in `/raw`

Use a single command with the `-p` flag to create the full, nested structure under `/raw` as defined in the final structure overview:

```bash
hdfs dfs -mkdir -p /raw/initial /raw/price /raw/forecast/weather-wind /raw/forecast/weather-temp /raw/forecast/weather-sun /raw/historical/weather-wind /raw/historical/weather-temp /raw/historical/weather-sun
```

## Automated Script Method

The entire creation process can be automated using the `create_hdfs_structure.sh` shell script, located in the `kubernetes/structure/` directory of the project.

### Step 1. Copy and Execute the Script

To automate the creation, copy the script to the **NameNode** pod and execute it from within.

#### 1.1. Copy the Script to the NameNode Pod

Run this command from your **local terminal**:

```bash
kubectl cp create_hdfs_structure.sh namenode-g5-0:/tmp/
```

### 1.2. Access the NameNode Pod

Access the NameNode pod directly from your terminal (if not already connected):

```bash
kubectl exec -it namenode-g5-0 -- bash
```

### 1.3. Execute the Script

Once inside the `root@namenode-g5-0:/#` shell prompt, make the script executable and run it:

```bash
chmod +x /tmp/create_hdfs_structure.sh
/tmp/create_hdfs_structure.sh
```


## Step 6. Verify the Created Directories

Use the following commands (executed inside the NameNode pod) to confirm the structure, regardless of the creation method used.

### 6.1. Verify Root Directories

```bash
hdfs dfs -ls /
```

Expected output (confirming top-level directories):

```
Found 4 items
drwxr-xr-x   - root supergroup          0 /analytics
drwxr-xr-x   - root supergroup          0 /historical
drwxr-xr-x   - root supergroup          0 /live
drwxr-xr-x   - root supergroup          0 /raw
```

### 6.2. Verify Nested Structure

To confirm the topic folders inside a specific year:

```bash
hdfs dfs -ls /historical/2022
```

Expected output:

```
Found 3 items
drwxr-xr-x   - root supergroup          0 /historical/2022/weather-sun
drwxr-xr-x   - root supergroup          0 /historical/2022/weather-temp
drwxr-xr-x   - root supergroup          0 /historical/2022/weather-wind
```

To confirm the nested structure in `/live`:

```bash
hdfs dfs -ls /live
```

Expected output:

```
Found 3 items
drwxr-xr-x   - root supergroup          0 ... /live/weather-sun
drwxr-xr-x   - root supergroup          0 ... /live/weather-temp
drwxr-xr-x   - root supergroup          0 ... /live/weather-wind
```

To confirm the nested structure in `/raw`:

```bash
hdfs dfs -ls /raw
```

Expected output (confirming sub-directories):

```
Found 4 items
drwxr-xr-x   - root supergroup          0 ... /raw/forecast
drwxr-xr-x   - root supergroup          0 ... /raw/historical
drwxr-xr-x   - root supergroup          0 ... /raw/initial
drwxr-xr-x   - root supergroup          0 ... /raw/price
```

-----

## Final Structure Overview

| Directory | Purpose |
| :--- | :--- |
| **`/raw`** | Stores raw ingested data (unprocessed weather, energy, or sensor files), with nested folders for `initial`, `forecast`, `price`, and `historical` raw files. |
| **`/historical`** | Stores historical weather data organized by year and topic (`/historical/YYYY/weather-*`). |
| **`/live`** | Contains real-time streaming weather data by topic (temperature, wind speed, solar energy). |
| **`/analytics`** | Stores processed and aggregated data used for analytics, dashboards, or reporting. |

### Example Folder Tree
```text
hdfs://namenode-g5:9000/
│
├── analytics/                           (empty - reserved)
│
├── historical/                          (~11 GB - enriched data)
│   ├── 2020/
│   │   ├── weather-wind/
│   │   ├── weather-temp/
│   │   └── weather-sun/
│   │
│   ├── 2021/
│   │   ├── weather-wind/
│   │   ├── weather-temp/
│   │   └── weather-sun/
│   │
│   ├── 2022/
│   │   ├── consumption/                   (5 months: 08-12)
│   │   │   ├── 08.avro/
│   │   │   ├── 09.avro/
│   │   │   ├── 10.avro/
│   │   │   ├── 11.avro/
│   │   │   └── 12.avro/
│   │   ├── weather-wind/                  (12 months: 01-12)
│   │   │   ├── 01.avro/
│   │   │   ├── 02.avro/
│   │   │   ├── ...
│   │   │   └── 12.avro/
│   │   ├── weather-temp/                  (12 months)
│   │   └── weather-sun/                   (12 months)
│   │
│   ├── 2023/
│   │   ├── consumption/                   (12 months: 01-12)
│   │   │   ├── 01.avro/
│   │   │   ├── 02.avro/
│   │   │   ├── ...
│   │   │   └── 12.avro/
│   │   ├── weather-wind/                  (12 months)
│   │   ├── weather-temp/                  (12 months)
│   │   └── weather-sun/                   (12 months)
│   │
│   ├── 2024/
│   │   ├── consumption/                   (12 months: 01-12)
│   │   ├── weather-wind/                  (12 months)
│   │   ├── weather-temp/                  (12 months)
│   │   └── weather-sun/                   (12 months)
│   │
│   ├── 2025/
│   │   ├── consumption/                   (11 months: 01-11)
│   │   ├── weather-wind/
│   │   │   ├── 01.avro/                  (batch processed)
│   │   │   └── 12_historical_streaming/   (live batches - growing)
│   │   │       ├── live_from_2025-12-05_16-30_batch-0_forecast-a94e5561/
│   │   │       ├── live_from_2025-12-05_16-31_batch-1_forecast-a94e5561/
│   │   │       ├── ...
│   │   │       └── live_from_2025-12-05_18-03_batch-XX_forecast-XXXXXXXX/
│   │   │
│   │   ├── weather-temp/
│   │   │   ├── 01.avro/
│   │   │   └── 12_historical_streaming/   (live batches)
│   │   │
│   │   └── weather-sun/
│   │       ├── 01.avro/
│   │       └── 12_historical_streaming/   (66 batches with 2 forecast IDs)
│   │           ├── live_from_2025-12-05_16-30_batch-0_forecast-a94e5561/
│   │           ├── ...
│   │           ├── live_from_2025-12-05_16-46_batch-32_forecast-a94e5561/  (first cycle)
│   │           ├── live_from_2025-12-05_17-24_batch-33_forecast-42abcd32/  (second cycle)
│   │           ├── ...
│   │           └── live_from_2025-12-05_17-40_batch-65_forecast-42abcd32/
│   │
│   └── live-archives/                     (completed forecast cycles)
│       └── 2025/
│           └── 12/
│               └── (empty - waiting for first cycle completion)
│
├── live/                                (273 MB - current cycle)
│   └── forecast/
│       ├── weather-wind/                  (33 part files, 105 MB)
│       │   ├── part-00000-00e2ce5c-...-c000.avro  (3.2 MB)
│       │   ├── part-00000-0194ee2d-...-c000.avro  (3.2 MB)
│       │   ├── ...
│       │   └── part-00000-f6e20d8b-...-c000.avro  (3.2 MB)
│       │
│       ├── weather-temp/                  (34 part files, 94 MB)
│       │   ├── part-00000-0013361b-...-c000.avro  (2.8 MB)
│       │   ├── part-00000-027cff7b-...-c000.avro  (2.8 MB)
│       │   ├── ...
│       │   ├── part-00000-919d4541-...-c000.avro  (21 KB - last batch)
│       │   └── part-00000-ff8f40ed-...-c000.avro  (2.8 MB)
│       │
│       └── weather-sun/                   (33 part files, 74 MB)
│           ├── part-00000-02c60d20-...-c000.avro  (2.3 MB)
│           ├── part-00000-05bfd522-...-c000.avro  (2.3 MB)
│           ├── ...
│           ├── part-00000-59b4a515-...-c000.avro  (1.1 MB - last batch)
│           └── part-00000-c921a3b1-...-c000.avro  (2.3 MB)
│
├── raw/                                 (2.1 GB - CSV files)
│   ├── initial-load/
│   │   ├── consumption/                   (38 files, 1.9 GB)
│   │   │   ├── consumption_2022_09.csv    (49.8 MB)
│   │   │   ├── consumption_2022_10.csv    (51.8 MB)
│   │   │   ├── ...
│   │   │   └── consumption_2025_11.csv    (30.6 MB)
│   │   │
│   │   ├── price/                         (10 files, 7.4 MB)
│   │   │   ├── DayAheadPrices_DK1_202101010000-202201010000.csv  (764 KB)
│   │   │   ├── DayAheadPrices_DK1_202201010000-202301010000.csv  (769 KB)
│   │   │   ├── ...
│   │   │   └── DayAheadPrices_DK2_202501010000-202601010000.csv  (544 KB)
│   │   │
│   │   ├── weather-wind/                  (5 files, 165 MB)
│   │   │   ├── 2020_dmi_wind.csv          (33.0 MB)
│   │   │   ├── 2021_dmi_wind.csv          (32.9 MB)
│   │   │   ├── 2022_dmi_wind.csv          (33.1 MB)
│   │   │   ├── 2023_dmi_wind.csv          (33.1 MB)
│   │   │   └── 2024_dmi_wind.csv          (33.3 MB)
│   │   │
│   │   ├── weather-temp/                  (5 files, 176 MB)
│   │   │   ├── 2020_dmi_temp.csv          (35.2 MB)
│   │   │   ├── 2021_dmi_temp.csv          (35.1 MB)
│   │   │   ├── 2022_dmi_temp.csv          (35.1 MB)
│   │   │   ├── 2023_dmi_temp.csv          (35.0 MB)
│   │   │   └── 2024_dmi_temp.csv          (35.3 MB)
│   │   │
│   │   └── weather-sun/                   (5 files, 81 MB)
│   │       ├── 2020_dmi_sun.csv           (16.3 MB)
│   │       ├── 2021_dmi_sun.csv           (16.2 MB)
│   │       ├── 2022_dmi_sun.csv           (16.2 MB)
│   │       ├── 2023_dmi_sun.csv           (16.2 MB)
│   │       └── 2024_dmi_sun.csv           (16.3 MB)
│   │
│   └── price/                             (empty)
│
├── tmp/                                (temporary files)
│
├── user/                                (user home directories)
│
└── utils/                               (utility scripts and reference data)
    └── municipality_codes_to_coordinates.csv  (98 municipalities, lat/lon mapping)
```

![diagram](./assets/HDFS_strucutre_diagram.png)
