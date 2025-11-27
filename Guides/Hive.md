# Finalized Guide: Hive Deployment and Initial CSV Load

This guide outlines the final steps to deploy your complete Hive infrastructure on Kubernetes and perform the initial load of CSV data from the Python Collector Sidecar into HDFS, ready for Spark processing.

## Phase 1: Deployment and Verification

### Step 1 — Apply the Complete Deployment YAML

Apply your hive-deployment.yaml file, which contains PostgreSQL, Hive Metastore, and the Hive Server Pod configured with the Python Collector Sidecar and the shared volume (/shared-data-for-hive).
```
kubectl apply -f hive-deployment.yaml
```

### Step 2 — Verify Pods Are Running

Check the status of all Pods in your namespace. The hive-server Pod must eventually show 2/2 Running (Hive Server and Python Collector Sidecar).
```
kubectl get pods -n bd-bd-gr-05
```

### Step 3 — Confirm Python Data Generation

Monitor the logs of the wind-collector Sidecar to confirm it successfully connected to the API and generated the CSV file in the shared volume.

# Use -c wind-collector to view the Python script logs
kubectl logs -n bd-bd-gr-05 deployment/hive-server -f -c wind-collector


Expected Log Output:
```
...
Summary: 55/57 stations have data
Saved 42735 wind records to shared volume path: /shared-data-for-hive/2020_dmi_wind.csv
...
Complete. DMI wind data saved to: /shared-data-for-hive/2020_dmi_wind.csv
Clean data ready for Hive processing!
```

## Phase 2: Data Transfer to HDFS

### Step 4 — Access Hive Shell (Crucial Correction)
Access the Hive shell by explicitly targeting the hive container.

```
kubectl exec -it deployment/hive-server -n bd-bd-gr-05 -c hive -- hive
```

Verify Hive is running:
```
SHOW DATABASES;
```

### Step 5 — Create Hive Database and CSV Table

Execute these commands in the Hive shell to create the database and the external table definition.

This table uses STORED AS TEXTFILE and FIELDS TERMINATED BY ',' to preserve the original CSV format on HDFS, as required for Spark consumption.
```
CREATE DATABASE IF NOT EXISTS dmi_wind;
USE dmi_wind;
```
```
-- CREATE EXTERNAL TABLE: Reads the CSV data structure
CREATE EXTERNAL TABLE IF NOT EXISTS dmi_wind.wind_raw_data (
  timeObserved STRING,
  stationId STRING,
  stationName STRING,
  mean_wind_speed DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION 'hdfs://namenode-g5:9000/raw/initial/dmi-wind-csv' 
TBLPROPERTIES ('skip.header.line.count'='1');
```

### Step 6 — Load CSV into HDFS via Hive

Use the LOAD DATA LOCAL command to move the file from the Pod's local shared volume (EmptyDir) into the HDFS directory associated with the table.

Command in Hive Shell (hive>):

```
LOAD DATA LOCAL INPATH '/shared-data-for-hive/2020_dmi_wind.csv' INTO TABLE dmi_wind.wind_raw_data;
```

## Phase 3: Verification

### Step 7 — Verify Data Count in Hive
Confirm that the data has been successfully loaded into the HDFS table (wind_raw_data).

Command in Hive Shell (hive>):

```
SELECT count(*) FROM dmi_wind.wind_raw_data;
SELECT * FROM dmi_wind.wind_raw_data LIMIT 10;
```

(The count should match the number of records saved by the Python script, approx. 42735)

### Step 8 — Verify CSV File in HDFS

Exit the Hive shell (!quit) and use the hdfs dfs -ls command (from the Hive container's bash) to confirm the CSV file is physically present in the HDFS directory.

```
kubectl exec -it deployment/hive-server -n bd-bd-gr-05 -c hive -- hdfs dfs -ls /raw/initial/dmi-wind-csv
```

Result: The listing should show the presence of the 2020_dmi_wind.csv file, confirming it is ready for your subsequent Spark job to transform it into Avro.
