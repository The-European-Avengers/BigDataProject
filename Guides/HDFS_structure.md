# Creating Directory Structure in HDFS

This guide explains how to create the base folder structure in HDFS and how to connect to the HDFS cluster to execute commands for creating directories.

## Prerequisites

Before you begin, make sure:

- Your HDFS cluster is up and running.
- The following pods are in `Running` state:

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


## Step 1. Access the NameNode Pod

To create directories in HDFS, you need to access the NameNode pod. In my case I decided to use the VSCode Kubernetes extension to open a terminal in the NameNode pod. Alternatively, you can use the following command to access the NameNode pod directly from your terminal:

```bash
kubectl exec -it namenode-g5-0 -- bash
```

You should now see a shell prompt like:

```
root@namenode-g5-0:/#
```


## Step 2. Create the HDFS Directory Structure

Use the `hdfs dfs -mkdir` command to create the top-level folders:

```bash
hdfs dfs -mkdir /raw /historical /live /analytics
```

This command creates the following directories in HDFS:

```
/raw
/historical
/live
/analytics
```


## Step 3. Verify the Created Directories

List the root directory in HDFS to confirm creation:

```bash
hdfs dfs -ls /
```

Expected output:

```
Found 4 items
drwxr-xr-x   - root supergroup          0 /analytics
drwxr-xr-x   - root supergroup          0 /historical
drwxr-xr-x   - root supergroup          0 /live
drwxr-xr-x   - root supergroup          0 /raw
```


## Step 4. Creating Yearly and Monthly Subdirectories in `/historical`

To organize historical data by year and month, create a nested folder structure under `/historical` directly from the NameNode pod.

### Create the Folder Structure

Run the following script **directly in the terminal** to create one folder per year (2020–2024) and one subfolder per month (`01`–`12`) inside each year:

```bash
for year in {2020..2024}; do
  for month in {01..12}; do
    hdfs dfs -mkdir -p /historical/$year/$month
  done
done
```

This command will create a structure similar to:

```
/historical/2020/01
/historical/2020/02
...
/historical/2024/12
```

### Verify the Result

List the contents of the `/historical` directory to confirm creation:

```bash
hdfs dfs -ls /historical
```

Expected output:

```
Found 5 items
drwxr-xr-x   - root supergroup          0 /historical/2020
drwxr-xr-x   - root supergroup          0 /historical/2021
drwxr-xr-x   - root supergroup          0 /historical/2022
drwxr-xr-x   - root supergroup          0 /historical/2023
drwxr-xr-x   - root supergroup          0 /historical/2024
```

To confirm the monthly folders, list a specific year:

```bash
hdfs dfs -ls /historical/2022
```

Expected output:

```
Found 12 items
drwxr-xr-x   - root supergroup          0 2025-11-04 15:18 /historical/2022/01
drwxr-xr-x   - root supergroup          0 2025-11-04 15:18 /historical/2022/02
drwxr-xr-x   - root supergroup          0 2025-11-04 15:18 /historical/2022/03
drwxr-xr-x   - root supergroup          0 2025-11-04 15:18 /historical/2022/04
drwxr-xr-x   - root supergroup          0 2025-11-04 15:18 /historical/2022/05
drwxr-xr-x   - root supergroup          0 2025-11-04 15:18 /historical/2022/06
drwxr-xr-x   - root supergroup          0 2025-11-04 15:18 /historical/2022/07
drwxr-xr-x   - root supergroup          0 2025-11-04 15:18 /historical/2022/08
drwxr-xr-x   - root supergroup          0 2025-11-04 15:19 /historical/2022/09
drwxr-xr-x   - root supergroup          0 2025-11-04 15:19 /historical/2022/10
drwxr-xr-x   - root supergroup          0 2025-11-04 15:19 /historical/2022/11
drwxr-xr-x   - root supergroup          0 2025-11-04 15:19 /historical/2022/12
```

## Final Structure Overview

| Directory     | Purpose                                                                                                              |
| ------------- | -------------------------------------------------------------------------------------------------------------------- |
| `/raw`        | Stores raw ingested data (unprocessed weather, energy, or sensor files)                                              |
| `/historical` | Stores historical weather, energy consumption, and pricing data, organized by year and month (`/historical/YYYY/MM`) |
| `/live`       | Contains streaming weather data (temperature, wind speed, solar energy)                                              |
| `/analytics`  | Stores processed and aggregated data for analysis and reporting                                                      |
