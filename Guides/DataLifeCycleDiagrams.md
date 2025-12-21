# Data Life Cycle Diagrams
```text
flowchart LR

%% INGESTION NODES
API[APIs]
Python[Python script]
Hive[Hive → HDFS]
KafkaHist[Kafka historical-&60;topic&62;]
KafkaLive[Kafka live-&60;topic&62;]
SparkHist[Spark streaming enrichment]
SparkLive[Spark streaming enrichment]

%% STORAGE NODES
HDFS_Raw[/HDFS /raw/]
HDFS_Historical[/HDFS /historical/]
HDFS_Live[/HDFS /live/]
HDFS_Archive[/HDFS /historical/archives/]
HDFS_Analytics[/HDFS /analytics/]

%% ML NODES
ML_Train[ML training &40;Spark job&41;]
ML_Predict[ML prediction &40;Spark job&41;]

%% PRECISION PIPELINE
PrecisionCalc[Prediction precision calculator]


%% ---------- INITIAL LOAD &40;one time&41; ----------
API --> Python
Python --> Hive
Hive --> HDFS_Raw
HDFS_Raw --> HDFS_Historical


%% ---------- HISTORICAL &40;6h weather / monthly consumption&41; ----------
API --> Python
Python --> KafkaHist
KafkaHist --> SparkHist
SparkHist --> HDFS_Historical


%% ---------- LIVE FORECAST &40;6h&41; ----------
API --> Python
Python --> KafkaLive
KafkaLive --> SparkLive
SparkLive --> HDFS_Live
SparkLive --> HDFS_Archive


%% ---------- MACHINE LEARNING PIPELINE ----------
HDFS_Historical --> ML_Train
HDFS_Live --> ML_Predict

ML_Train --> HDFS_Archive
ML_Predict --> HDFS_Analytics


%% ---------- PRECISION CALCULATION LOOP ----------
HDFS_Analytics --> PrecisionCalc
HDFS_Historical --> PrecisionCalc
PrecisionCalc --> HDFS_Analytics
```

## Initial-Load Diagram
```text
sequenceDiagram
    title Initial Load Data Sequence

    participant API as External APIs
    participant Python as Python Script
    participant CSV as CSV Files
    participant Hive as Hive Ingestion
    participant Raw as HDFS /raw/
    participant Spark as Spark Batch Job
    participant Hist as HDFS /historical/<year>/<topic>/

    API->>Python: Retrieve full historical dataset (JSON)
    Python->>CSV: Transform data into CSV format
    CSV->>Hive: Load CSV into Hive tables
    Hive->>Raw: Store data in HDFS /raw/ area
    Spark->>Raw: Read raw data from HDFS
    Spark->>Spark: Enrich data (municipalityCode and dkArea)
    Spark->>Hist: Write monthly Avro files to /historical/<year>/<topic>/
```

## Historical Diagram
```text
sequenceDiagram
    title Historical Data Sequence

    participant API as External APIs
    participant Python as Python Script
    participant Kafka as Kafka (historical-<topic>)
    participant Spark as Spark Streaming Job
    participant Hist as HDFS /historical/<year>/<topic>/

    API->>Python: Retrieve new historical data (every 24h)
    Python->>Kafka: Send Avro messages to historical-<topic> topic
    Spark->>Kafka: Consume historical events
    Spark->>Spark: Enrich data (municipalityCode and dkArea)
    Spark->>Hist: Append to monthly Avro files in /historical/<year>/<topic>/
```

## Live Diagram
```text
sequenceDiagram
    title Live Data Sequence

    participant API as External APIs
    participant Python as Python Script
    participant Kafka as Kafka (live-<topic>)
    participant Stream as Spark Streaming Job
    participant Live as HDFS /live/
    participant Archive as HDFS /historical/archives/<year>/<month>/live/
    participant Batch as Spark Batch Job
    participant Forecast as HDFS /historical/<year>/forecast-<topic>/

    API->>Python: Retrieve live forecast data (every 6 hours)
    Python->>Kafka: Send Avro messages to live-<topic> topic
    Stream->>Kafka: Consume live forecast events
    Stream->>Stream: Enrich data (municipalityCode and dkArea)
    Stream->>Live: Overwrite Avro files in /live/
    Stream->>Archive: Store copy in monthly archive folder
    Batch->>Live: Read latest live data
    Batch->>Forecast: Produce monthly Avro files in forecast-<topic> folder
```