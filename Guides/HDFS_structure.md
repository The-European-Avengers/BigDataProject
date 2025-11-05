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


## Step 4. Creating Yearly and Topic Subdirectories in `/historical`

To organize historical data by **year** and **topic**, create a nested folder structure under `/historical` directly from the NameNode pod.

### Create the Folder Structure

Run the following script **directly in the terminal** to create one folder per year (2020–2024) and one subfolder per topic (`weather-wind`, `weather-temp`, `weather-sun`) inside each year:

```bash
for year in {2020..2024}; do
  for topic in weather-wind weather-temp weather-sun; do
    hdfs dfs -mkdir -p /historical/$year/$topic
  done
done
```

This command will create a structure similar to:

```
/historical/2020/weather-wind
/historical/2020/weather-temp
/historical/2020/weather-sun
...
/historical/2024/weather-wind
/historical/2024/weather-temp
/historical/2024/weather-sun
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

To confirm the topic folders inside a year:

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

Inside each topic folder, there will be 12 `avro` files representing monthly data. The name of each file will specify the month of the data it contains.

## Step 5. Creating Topic Subdirectories in `/live`
Similarly, create topic subdirectories under the `/live` directory for real-time data:

```bash
hdfs dfs -mkdir /live/weather-temp
hdfs dfs -mkdir /live/weather-wind
hdfs dfs -mkdir /live/weather-sun 
```

To verify:

```bash
hdfs dfs -ls /live
```
Expected output:

```
Found 3 items
drwxr-xr-x   - root supergroup          0 2025-11-05 13:01 /live/weather-sun
drwxr-xr-x   - root supergroup          0 2025-11-05 13:01 /live/weather-temp
drwxr-xr-x   - root supergroup          0 2025-11-05 13:01 /live/weather-wind
```

## Final Structure Overview

| Directory     | Purpose                                                                                   |
| ------------- | ----------------------------------------------------------------------------------------- |
| `/raw`        | Stores raw ingested data (unprocessed weather, energy, or sensor files)                   |
| `/historical` | Stores historical weather data organized by year and topic (`/historical/YYYY/weather-*`) |
| `/live`       | Contains real-time streaming weather data by topic (temperature, wind speed, solar energy)         |
| `/analytics`  | Stores processed and aggregated data used for analytics, dashboards, or reporting         |


## Example Folder Tree

```text
/
├── raw
├── historical
│   ├── 2020
│   │   ├── weather-wind
│   │   ├── weather-temp
│   │   └── weather-sun
│   ├── 2021
│   │   ├── weather-wind
│   │   ├── weather-temp
│   │   └── weather-sun
│   ├── 2022
│   │   ├── weather-wind
│   │   ├── weather-temp
│   │   └── weather-sun
│   ├── 2023
│   │   ├── weather-wind
│   │   ├── weather-temp
│   │   └── weather-sun
│   └── 2024
│       ├── weather-wind
│       ├── weather-temp
│       └── weather-sun
├── live
│   ├── weather-wind
│   ├── weather-temp
│   └── weather-sun
└── analytics
```                                                  
![diagram](./assets/HDFS_strucutre_diagram.png)



