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
/
├── raw
│   ├── initial-load
│   │   ├── weather-wind
│   │   ├── weather-temp
│   │   ├── weather-sun
│   │   └── heating-consumption
│   ├── forecast
│   │   ├── weather-wind
│   │   ├── weather-temp
│   │   └── weather-sun
│   ├── price
│   └── historical
│       ├── weather-wind
│       ├── weather-temp
│       └── weather-sun
├── historical
│   ├── 2020/2021/2022/2023/2024
│   │   ├── weather-wind
│   │   ├── weather-temp
│   │   └── weather-sun
├── live
│   ├── weather-wind
│   ├── weather-temp
│   └── weather-sun
└── analytics
```

![diagram](./assets/HDFS_strucutre_diagram.png)