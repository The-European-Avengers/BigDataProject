
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

## Step 4 — Check the python script works



```bash
kubectl logs -n bd-bd-gr-05 deployment/hive-server -f
```
You have to get this:
```
Saved 42735 wind records to shared volume path: /shared-data-for-hive/2020_dmi_wind.csv
Date range: 2020-01-01 00:00:00+00:00 to 2020-02-01 00:00:00+00:00
Stations: 55
Average mean_wind_speed: 6.42
Max mean_wind_speed: 20.30
Total time: 0:00:18.032063
Complete. DMI wind data saved to: /shared-data-for-hive/2020_dmi_wind.csv
Clean data ready for Hive processing!
```


### Step 4 — Verify CSV on Shared Volume (Nuevo)

Verifica que el archivo CSV esté accesible en el sistema de archivos local del Pod, en el _path_ donde montamos el volumen compartido (`/shared-data-for-hive/`).

**Salir de la shell de Hive (`!quit`) y ejecutar en Bash:**

Bash

```
# Entrar al Bash del contenedor Hive
kubectl exec -it deployment/hive-server -n bd-bd-gr-05 -c hive -- bash
```
```
# Listar el contenido del volumen compartido
ls -l /shared-data-for-hive
```
```
# Resultado esperado:
# -rw-r--r-- 1 root root [tamaño] [fecha] 2020_dmi_wind.csv
```

---

### Step 5 — Create Hive Database and Table (Adaptado)

Vuelve a entrar a la _shell_ de Hive y ejecuta los comandos para preparar las tablas.

**Comandos en Hive Shell (`hive>`):**

SQL

```
CREATE DATABASE IF NOT EXISTS weather_data;
USE weather_data;
```
```

CREATE EXTERNAL TABLE IF NOT EXISTS weather_data.wind_raw_data (
  timeObserved STRING,
  stationId STRING,
  stationName STRING,
  mean_wind_speed DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION 'hdfs://namenode-g5:9000/raw/initial/weather-wind'
TBLPROPERTIES ('skip.header.line.count'='1'); 
```

---

### Step 6 — Load CSV into HDFS via Hive (Corregido)

Carga el archivo desde el volumen compartido (local al Pod) hacia la ubicación HDFS definida en la tabla `wind_raw_data`.

**Comando en Hive Shell (`hive>`):**

SQL

```
LOAD DATA LOCAL INPATH '/shared-data-for-hive/2020_dmi_wind.csv' INTO TABLE weather_data.wind_raw_data;
```

**Verify in Hive:**

SQL

```
-- Verifica que los datos son legibles
SELECT count(*) FROM weather_data.wind_raw_data;
SELECT * FROM weather_data.wind_raw_data LIMIT 10;
```

**Verify in HDFS (Desde el contenedor Hive):**

Sal de la _shell_ de Hive (`!quit`) y ejecuta en Bash:

Bash

```
hdfs dfs -ls /raw/initial/weather-wind
```

**Resultado esperado:**

```
Found 1 items
-rwxr-xr-x   3 root supergroup       [tamaño] [fecha] /raw/dmi_wind/csv/2020_dmi_wind.csv
```
