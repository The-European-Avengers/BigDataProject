# Guide to set up Hive on Kubernetes

This guide walks you through deploying Hive on Kubernetes with an embedded PostgreSQL metastore and loading CSV data into HDFS.

---

## Step 1 — Apply the Complete Hive Deployment YAML

Your single `hive-deployment.yaml` now includes:

1. PostgreSQL PVC, Deployment, and Service  
2. Hadoop & Hive ConfigMap  
3. Hive Metastore Deployment & Service  
4. Hive Server Deployment & Service  

Apply the deployment:

```bash
kubectl apply -f hive-deployment.yaml
````

---

## Step 2 — Verify Pods Are Running

Check that all pods in namespace `bd-bd-gr-05` are running:

```bash
kubectl get pods -n bd-bd-gr-05
```

You should see:

- `postgresql-*`
    
- `hive-metastore-*`
    
- `hive-server-*`
    

> The initContainers will wait for PostgreSQL and the Metastore to become ready before starting the main containers.

---

## Step 3 — Access Hive Shell

Open a Hive shell inside the Hive Server pod:

```bash
kubectl exec -it deployment/hive-server -n bd-bd-gr-05 -- hive
```

Verify Hive is running:

```sql
SHOW DATABASES;
```

Expected output:

```
hive> SHOW DATABASES;
OK
default
Time taken: 0.737 seconds, Fetched: 1 row(s)
```

---

## Step 4 — Upload CSV to Hive Pod

Hive’s `LOAD DATA LOCAL` reads files inside the Hive pod filesystem. Copy your CSV:

```bash
kubectl cp ./weather.csv bd-bd-gr-05/hive-server-<pod-id>:/tmp/weather.csv
```

---

## Step 5 — Create Hive Database and Table

From the Hive shell:

```sql
CREATE DATABASE IF NOT EXISTS weather;

CREATE EXTERNAL TABLE IF NOT EXISTS weather.raw_data (
  reading_date STRING,
  hour INT,
  from_time STRING,
  to_time STRING,
  station_id INT,
  station_name STRING,
  parameter STRING,
  value DOUBLE,
  latitude DOUBLE,
  longitude DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/raw/initial/';
```

**Notes:**

- `EXTERNAL` prevents Hive from taking full ownership of files.
    
- `LOCATION` explicitly ties the table to the directory in HDFS.
    

---

## Step 6 — Load CSV into HDFS via Hive

From the same Hive shell:

```sql
LOAD DATA LOCAL INPATH '/tmp/weather.csv' INTO TABLE weather.raw_data;
```

**Verify in Hive:**

```sql
SELECT * FROM weather.raw_data LIMIT 10;
```

**Verify in HDFS (from any HDFS-capable pod):**

```bash
hdfs dfs -ls /raw/initial
```

Example output:

```
Found 1 items
-rwxr-xr-x   3 root supergroup       1147 2025-11-12 12:00 /raw/initial/weather.csv
```

Check content:

```bash
hdfs dfs -cat /raw/initial/weather.csv
```
