# Big Data Project - Comprehensive Architecture & Data Flow Summary

## Table of Contents
1. [HDFS Directory Structure](#hdfs-directory-structure)
2. [Hive External Tables & Schemas](#hive-external-tables--schemas)
3. [Redis Configuration & Caching Strategy](#redis-configuration--caching-strategy)
4. [Backend API Endpoints](#backend-api-endpoints)
5. [Frontend Components & Metrics](#frontend-components--metrics)
6. [Data Flow Architecture](#data-flow-architecture)

---

## HDFS Directory Structure

### Overview
HDFS is organized into 4 main directories, each serving a specific purpose:

```
/
├── /raw                    # Raw ingested data (unprocessed)
│   ├── /initial/          # Initial data loads
│   ├── /price/            # Price data
│   ├── /forecast/         # Weather forecast data
│   │   ├── /weather-wind/
│   │   ├── /weather-temp/
│   │   └── /weather-sun/
│   └── /historical/       # Historical weather data
│       ├── /weather-wind/
│       ├── /weather-temp/
│       └── /weather-sun/
│
├── /historical            # Organized historical data by year/topic
│   ├── /2020/
│   ├── /2021/
│   ├── /2022/
│   ├── /2023/
│   └── /2024/
│       ├── /consumption/{MONTH}.avro  # Monthly consumption AVRO files
│       ├── /weather-wind/
│       ├── /weather-temp/
│       └── /weather-sun/
│
├── /live                  # Real-time streaming data
│   ├── /weather-temp/
│   ├── /weather-wind/
│   └── /weather-sun/
│
└── /analytics             # Processed & aggregated data for analytics
```

### Key Details
- **Replication Factor**: 3 (for fault tolerance)
- **Access**: Via NameNode pod `namenode-g5-0`
- **NameNode Address**: `hdfs://namenode-g5:9000`
- **Data Formats**: CSV (raw), AVRO (processed), TEXTFILE, Parquet

### Commands to Verify Structure
```bash
# Access NameNode
kubectl exec -it namenode-g5-0 -- bash

# List root directories
hdfs dfs -ls /

# Check specific year data
hdfs dfs -ls /historical/2024/consumption/

# Check data size
hdfs dfs -du -h /historical/2024/consumption/
```

---

## Hive External Tables & Schemas

### External Table Definition
Hive uses **external tables** that reference data stored in HDFS. External tables don't delete data when dropped.

### Table Schemas

#### 1. **Wind Data** (`dmi_wind.wind_raw_data`)
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS dmi_wind.wind_raw_data (
  timeObserved STRING,
  stationId STRING,
  stationName STRING,
  mean_wind_speed DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode-g5:9000/raw/initial-load/weather-wind'
TBLPROPERTIES ('skip.header.line.count'='1');
```
- **Source**: CSV files from DMI (Danish Meteorological Institute)
- **HDFS Location**: `/raw/initial-load/weather-wind`
- **Fields**: 4 columns (timestamp, station ID, station name, wind speed)

#### 2. **Temperature Data** (`dmi_temp.temp_raw_data`)
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS dmi_temp.temp_raw_data (
  timeObserved STRING,
  stationId STRING,
  stationName STRING,
  mean_temp DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode-g5:9000/raw/initial-load/weather-temp'
TBLPROPERTIES ('skip.header.line.count'='1');
```
- **HDFS Location**: `/raw/initial-load/weather-temp`
- **Fields**: Temperature in Celsius

#### 3. **Solar Radiation Data** (`dmi_sun.sun_raw_data`)
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS dmi_sun.sun_raw_data (
  timeObserved STRING,
  stationId STRING,
  stationName STRING,
  mean_radiation DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode-g5:9000/raw/initial-load/weather-sun'
TBLPROPERTIES ('skip.header.line.count'='1');
```
- **HDFS Location**: `/raw/initial-load/weather-sun`
- **Fields**: Solar radiation in W/m²

#### 4. **Heating Consumption Data** (`energy_heating.heating_consumption_raw`)
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS energy_heating.heating_consumption_raw (
  ConsumptionkWh DOUBLE,
  HeatingCategory STRING,
  HousingCategory STRING,
  Municipality STRING,
  MunicipalityCode INT,
  RegionName STRING,
  TimeDK STRING,
  TimeUTC STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode-g5:9000/raw/initial-load/heating-consumption'
TBLPROPERTIES ('skip.header.line.count'='1');
```
- **HDFS Location**: `/raw/initial-load/heating-consumption`
- **Fields**: 8 columns including consumption, category, municipality codes

#### 5. **Analytics Predictions** (`analytics.predictions`) - GENERATED
```
Schema (Mock in Dashboard):
- timestamp: ISO string
- municipalityCode: INT
- dkArea: STRING (DK1 or DK2)
- consumptionkWh: DOUBLE
- mean_temp: DOUBLE
- mean_radiation: DOUBLE
- mean_wind_speed: DOUBLE
- productionkWh: DOUBLE
- price: DOUBLE
- consumptionPrecision: DOUBLE
- pricePrecision: DOUBLE
```

#### 6. **Consumption Data** (`analytics.consumption_data`)
```
Schema (Mock in Dashboard):
- timestamp: ISO string
- municipalityCode: INT
- consumptionkWh: DOUBLE
- precision: DOUBLE
```

#### 7. **Weather Data** (`analytics.weather_data`)
```
Schema (Mock in Dashboard):
- timestamp: ISO string
- dkArea: STRING (DK1 or DK2)
- mean_temp: DOUBLE
- mean_radiation: DOUBLE
- mean_wind_speed: DOUBLE
```

### Accessing Hive Tables
```bash
# Access Hive shell
kubectl exec -it deployment/hive-server -n bd-bd-gr-05 -c hive -- hive

# Show databases
SHOW DATABASES;

# Show tables in database
SHOW TABLES IN analytics;

# Query table
SELECT * FROM analytics.predictions LIMIT 10;

# Show table structure
DESCRIBE analytics.predictions;
```

---

## Redis Configuration & Caching Strategy

### Architecture
```
Frontend (React)
    ↓ HTTP REST API
NestJS Backend
    ↓ (Check cache first)
Redis Store (6379)
    ↓ (If cache miss)
Hive Server (10000)
    ↓ (Query execution)
HDFS Data
```

### Configuration

#### Environment Variables
```env
# File: Dashboard/backend/.env
REDIS_HOST=redis-service
REDIS_PORT=6379
REDIS_DB=0
CACHE_TTL=3600  # 1 hour in seconds
```

#### Kubernetes Configuration
**File**: [Dashboard/k8s/configmap.yaml](Dashboard/k8s/configmap.yaml)
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: dashboard-config
  namespace: bd-bd-gr-05
data:
  REDIS_HOST: "redis-service"
  REDIS_PORT: "6379"
  REDIS_DB: "0"
  HIVE_HOST: "hive-server"
  HIVE_PORT: "10000"
  CACHE_TTL: "3600"
```

### Redis Service Implementation

**File**: [Dashboard/backend/src/modules/redis/redis.service.ts](Dashboard/backend/src/modules/redis/redis.service.ts)

#### Key Methods:
```typescript
// Redis Service Class
export class RedisService implements OnModuleInit, OnModuleDestroy {
  private client: RedisClient;
  private isConnected: boolean;

  // Initialize connection on module load
  async onModuleInit(): Promise<void>

  // Get value from cache
  async get(key: string): Promise<any>

  // Set value with optional TTL
  async set(key: string, value: any, ttl?: number): Promise<void>

  // Delete key from cache
  async del(key: string): Promise<void>

  // Flush entire database
  async flush(): Promise<void>
}
```

#### Connection Details:
- **Library**: `redis` npm package (v4.6.10)
- **Port**: 6379
- **Connection Timeout**: 5000ms
- **Reconnection**: Exponential backoff (max 500ms)
- **Health Check**: Automatic retry strategy

### Caching Strategy

#### Cache Keys Format
```
hive:tables                                    # List of all tables
hive:table:{database}.{tableName}:{limit}     # Table query results
hive:predictions:{limit}:{filters}            # Predictions with filters
```

#### Cache Flow
1. **Get Cache**: Check Redis for key with TTL
2. **Cache Miss**: Execute Hive query
3. **Store Result**: Save to Redis with TTL (default 3600 seconds)
4. **Return Data**: Send to frontend

#### Cache Invalidation
```bash
# Invalidate specific table cache
POST /api/hive/cache/invalidate?table=predictions

# Clears:
- hive:table:analytics.predictions:100
- hive:predictions:100:{}
- hive:tables (indirect)
```

#### Cache Benefits
- **Reduced Load**: Queries cached for 1 hour
- **Faster Response**: ~10ms for cached vs 500ms+ for live queries
- **Resilience**: Works without Redis (graceful degradation)

---

## Backend API Endpoints

### Base URL
- **Development**: `http://localhost:3000`
- **Kubernetes**: `http://backend-service:3000`

### Health & Status

#### Health Check
```http
GET /health

Response: 200 OK
```

#### Swagger Documentation
```http
GET /api/docs
GET /api-json
```

### Hive Data Endpoints

#### 1. Get Available Tables
```http
GET /api/hive/tables

Response:
{
  "data": ["predictions", "consumption_data", "weather_data"]
}
```
- **Cache Key**: `hive:tables`
- **TTL**: 3600 seconds
- **Source**: [hive.controller.ts](Dashboard/backend/src/modules/hive/hive.controller.ts#L14)

#### 2. Query Specific Table
```http
GET /api/hive/table/:tableName?limit=100&database=analytics

Parameters:
- tableName (required): Table name
- limit (optional): Number of rows (default: 100)
- database (optional): Database name (default: "analytics")

Response:
{
  "database": "analytics",
  "tableName": "consumption_data",
  "limit": 100,
  "rowCount": 30,
  "columns": ["timestamp", "municipalityCode", "consumptionkWh", "precision"],
  "rows": [
    {
      "timestamp": "2024-12-20T00:00:00Z",
      "municipalityCode": 101,
      "consumptionkWh": 2850.50,
      "precision": 0.95
    }
  ]
}
```
- **Cache Key**: `hive:table:analytics.{tableName}:{limit}`
- **TTL**: 3600 seconds

#### 3. Get Consumption Data
```http
GET /api/hive/consumption?limit=100

Response: Same as table query for consumption_data
```

#### 4. Get Weather Data
```http
GET /api/hive/weather?limit=100

Response: Same as table query for weather_data
```

#### 5. Get Predictions (with Filters)
```http
GET /api/hive/predictions?limit=100&dkArea=DK1&municipalityCode=101

Parameters:
- limit (optional): Number of rows (default: 100)
- dkArea (optional): "DK1" or "DK2"
- municipalityCode (optional): Numeric municipality code

Response:
{
  "database": "analytics",
  "tableName": "predictions",
  "limit": 100,
  "rowCount": 45,
  "columns": ["timestamp", "municipalityCode", "dkArea", "consumptionkWh", 
              "mean_temp", "mean_radiation", "mean_wind_speed", 
              "productionkWh", "price", "consumptionPrecision", "pricePrecision"],
  "rows": [
    {
      "timestamp": "2024-12-20T00:00:00Z",
      "municipalityCode": 101,
      "dkArea": "DK1",
      "consumptionkWh": 2850.50,
      "mean_temp": 5.2,
      "mean_radiation": 120.5,
      "mean_wind_speed": 8.3,
      "productionkWh": 1250.75,
      "price": 1.85,
      "consumptionPrecision": 0.95,
      "pricePrecision": 0.98
    }
  ],
  "filters": {
    "dkArea": "DK1",
    "municipalityCode": 101
  }
}
```
- **Cache Key**: `hive:predictions:{limit}:{dkArea}:{municipalityCode}`
- **TTL**: 3600 seconds
- **Source**: [hive.controller.ts](Dashboard/backend/src/modules/hive/hive.controller.ts#L41)

#### 6. Invalidate Cache
```http
POST /api/hive/cache/invalidate?table=predictions

Response:
{
  "message": "Cache invalidated"
}
```
- **Description**: Clears Redis cache for specific table or all if omitted
- **Usage**: After data updates in Hive

### Service Implementation

**File**: [Dashboard/backend/src/modules/hive/hive.service.ts](Dashboard/backend/src/modules/hive/hive.service.ts)

#### Core Methods:
```typescript
class HiveService {
  // Get all available tables (cached)
  async getTables(): Promise<string[]>

  // Query any table with filters
  async queryTable(tableName, limit, database): Promise<TableData>

  // Query predictions with DK area and municipality filters
  async queryPredictions(limit, dkArea?, municipalityCode?): Promise<PredictionData>

  // Execute actual Hive query
  private async executeHiveQuery(query, host, port, database, tableName): Promise<any>

  // Invalidate cache for table
  async invalidateCache(tableName?): Promise<void>
}
```

#### Mock Data Generation
When Hive is unavailable, the service generates realistic mock data:
- **Timestamps**: Last 30 days
- **Consumption**: 2000-5000 kWh range
- **Price**: 0.5-3 DKK/kWh
- **Temperature**: -10 to 25°C
- **Wind**: 0-15 m/s
- **Radiation**: 0-800 W/m²

---

## Frontend Components & Metrics

### Architecture
```
React Frontend (Port 3001)
    ├── App.js (Router)
    ├── pages/
    │   └── Dashboard.js
    ├── components/
    │   ├── PredictionsTimeline.js       ← Predictions visualization
    │   ├── TableList.js                 ← Available tables list
    │   ├── TableViewer.js               ← Generic table viewer
    │   └── Navigation.js                ← Header navigation
    └── services/
        ├── hiveService.js               ← API wrapper
        └── api.js                       ← Axios instance
```

### Key Components

#### 1. **PredictionsTimeline** Component
**File**: [Dashboard/frontend/src/components/PredictionsTimeline.js](Dashboard/frontend/src/components/PredictionsTimeline.js)

**Purpose**: Visualize energy predictions over time with multiple metrics

**Metrics Displayed**:
```javascript
metricConfig = {
  consumption: {
    title: "Energy Consumption",
    label: "Consumption (kWh)",
    key: "consumptionkWh",
    color: "#8884d8",      // Blue
    yAxis: "left"
  },
  price: {
    title: "Energy Price",
    label: "Price (DKK)",
    key: "price",
    color: "#ffc658",      // Orange
    yAxis: "right"
  },
  production: {
    title: "Energy Production",
    label: "Production (kWh)",
    key: "productionkWh",
    color: "#82ca9d",      // Green
    yAxis: "left"
  }
}
```

**Features**:
- 📈 **Interactive Line Chart**: Using Recharts library
- ✓ **Multi-metric Selection**: Toggle consumption, price, production
- 🔍 **DK Area Filter**: Filter by DK1 or DK2 (electrical zones)
- 📊 **Statistics Display**:
  - Average consumption/production
  - Total consumption/production
  - Min/max prices
- 🔄 **Auto-refresh**: Loads data on component mount
- ⏳ **Loading States**: Shows loading indicator while fetching

**Data Flow**:
```javascript
1. Component mounts → fetchPredictions()
2. Call: GET /api/hive/predictions?limit=100&dkArea={dkArea}
3. Parse response → Sort by timestamp
4. Transform for chart → setChartData()
5. Render LineChart with selected metrics
6. Update on DK Area filter change
```

**Data Transformation**:
```javascript
// Raw data from API
[
  { timestamp: "2024-12-20T00:00:00Z", consumptionkWh: 2850.50, ... }
]

// Transformed for chart
[
  { 
    timestamp: "Dec 20",           // Short format for X-axis
    fullDate: "2024-12-20T00:00:00Z",
    consumptionkWh: 2850.50,
    price: 1.85,
    productionkWh: 1250.75,
    dkArea: "DK1",
    municipalityCode: 101
  }
]
```

#### 2. **TableList** Component
**File**: [Dashboard/frontend/src/components/TableList.js](Dashboard/frontend/src/components/TableList.js)

**Purpose**: Display available Hive tables as sidebar navigation

**Features**:
- 📋 Lists all queryable tables: `["predictions", "consumption_data", "weather_data"]`
- 🔄 Refresh button to reload list
- 🎯 Click to select table for viewing
- ⚠️ Error handling and loading states

**Data Flow**:
```javascript
1. Component mounts → fetchTables()
2. Call: GET /api/hive/tables
3. Parse response → setTables()
4. Render as clickable list
5. onClick → onSelectTable(tableName)
```

#### 3. **TableViewer** Component
**File**: [Dashboard/frontend/src/components/TableViewer.js](Dashboard/frontend/src/components/TableViewer.js)

**Purpose**: Generic table data viewer for any Hive table

**Features**:
- 📊 Displays table data in HTML table format
- 📈 Shows row/column counts
- 🔄 Refresh button
- ✕ Clear cache button (invalidates Redis cache)
- 📜 Scrollable table for large datasets
- 🎯 Responsive column display

**Data Flow**:
```javascript
1. Receive tableName prop
2. useEffect → fetchTableData()
3. Call: GET /api/hive/table/{tableName}?limit=100
4. Render columns in <thead>
5. Render rows in <tbody>
6. Handle cache invalidation
```

#### 4. **Navigation** Component
**File**: [Dashboard/frontend/src/components/Navigation.js](Dashboard/frontend/src/components/Navigation.js)

**Purpose**: Header navigation and branding

**Features**:
- 📍 Dashboard title and branding
- 🌐 Links to documentation
- 💻 System status indicator

#### 5. **Dashboard** Page
**File**: [Dashboard/frontend/src/pages/Dashboard.js](Dashboard/frontend/src/pages/Dashboard.js)

**Layout**:
```
┌─────────────────────────────────────────┐
│          HDFS Dashboard                  │
├──────────────┬──────────────────────────┤
│              │                          │
│  TableList   │   Main Content          │
│  (Sidebar)   │  (PredictionsTimeline   │
│              │   or TableViewer)       │
│              │                          │
└──────────────┴──────────────────────────┘
```

**States**:
- Selected table from TableList
- If `predictions` → Show PredictionsTimeline
- Otherwise → Show TableViewer with generic table

### API Service Layer

**File**: [Dashboard/frontend/src/services/hiveService.js](Dashboard/frontend/src/services/hiveService.js)

```javascript
const hiveService = {
  // Get list of tables
  getTables: async () => {
    // GET /api/hive/tables
  },

  // Query specific table
  queryTable: async (tableName, limit = 100) => {
    // Routes to appropriate endpoint based on table name:
    // - "predictions" → GET /api/hive/predictions
    // - "consumption_data" → GET /api/hive/consumption
    // - "weather_data" → GET /api/hive/weather
    // - others → GET /api/hive/table/{tableName}
  },

  // Invalidate cache for table
  invalidateCache: (tableName) => {
    // POST /api/hive/cache/invalidate?table={tableName}
  }
}
```

**File**: [Dashboard/frontend/src/services/api.js](Dashboard/frontend/src/services/api.js)

```javascript
// Axios instance configured with:
// - Base URL: http://localhost:3000
// - Default headers
// - Error handling
```

### Frontend Data Display Summary

| Component | Data Source | Metrics/Fields Displayed |
|-----------|------------|--------------------------|
| **PredictionsTimeline** | `/api/hive/predictions` | Consumption (kWh), Price (DKK), Production (kWh), Temperature, Wind Speed, Solar Radiation |
| **TableViewer (predictions)** | `/api/hive/predictions` | All columns from predictions table |
| **TableViewer (consumption)** | `/api/hive/consumption` | Timestamp, Municipality Code, Consumption (kWh), Precision |
| **TableViewer (weather)** | `/api/hive/weather` | Timestamp, DK Area, Mean Temp, Mean Radiation, Mean Wind Speed |

---

## Data Flow Architecture

### End-to-End Data Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                             │
├─────────────────────────────────────────────────────────────┤
│ DMI Weather Data    Energinet Price Data    Consumption Data │
│ (Wind, Temp, Sun)   (DayAheadPrices)      (Municipality)     │
└────────────┬──────────────────┬──────────────────┬───────────┘
             │                  │                  │
             └──────────────────┼──────────────────┘
                                │ CSV Files
                                │ Uploaded to K8s
                                │
                                ▼
                    ┌───────────────────────┐
                    │  Shared Data Volume   │
                    │  (/shared-data-for)   │
                    │      -hive/)          │
                    └──────────┬────────────┘
                               │ Hive LOAD
                               │ DATA LOCAL
                               │ INPATH
                               ▼
                ┌──────────────────────────────┐
                │  HDFS (/raw/initial-load/*)  │
                │  - weather-wind/             │
                │  - weather-temp/             │
                │  - weather-sun/              │
                │  - heating-consumption/      │
                │  - price/                    │
                └──────────┬───────────────────┘
                           │ Hive External
                           │ Tables Read
                           │ (TEXTFILE)
                           ▼
                ┌──────────────────────────────┐
                │  Hive Databases & Tables     │
                │  - dmi_wind.wind_raw_data    │
                │  - dmi_temp.temp_raw_data    │
                │  - dmi_sun.sun_raw_data      │
                │  - energy_heating.*          │
                │  - analytics.predictions     │
                └──────────┬───────────────────┘
                           │ SQL Query
                           │ SELECT * FROM
                           ▼
                ┌──────────────────────────────┐
                │  HiveServer2 (Port 10000)    │
                │  Connection: Thrift Protocol │
                └──────────┬───────────────────┘
                           │ Query Result
                           │ (JSON/Rows)
                           ▼
            ┌─────────────────────────────────┐
            │     Redis Cache (Port 6379)     │
            │     TTL: 3600 seconds (1 hour)  │
            │     Keys: hive:table:*, hive:*  │
            └──────────────┬──────────────────┘
                           │ Cached
                           │ Response
                           │ or Miss
                           ▼
            ┌─────────────────────────────────┐
            │  NestJS Backend (Port 3000)     │
            │  - HiveService (Query Logic)    │
            │  - RedisService (Cache Mgmt)    │
            │  - HiveController (REST API)    │
            └──────────────┬──────────────────┘
                           │ HTTP REST
                           │ JSON Response
                           ▼
            ┌─────────────────────────────────┐
            │  React Frontend (Port 3001)     │
            │  - PredictionsTimeline (Chart)  │
            │  - TableViewer (Tables)         │
            │  - TableList (Navigation)       │
            └──────────────┬──────────────────┘
                           │ Rendered
                           │ Interactive UI
                           ▼
                    ┌──────────────┐
                    │  User Browser │
                    │  - Charts     │
                    │  - Tables     │
                    │  - Filters    │
                    └───────────────┘
```

### Query Request Flow (in detail)

```
1. USER INTERACTION
   ├─ Click "Predictions" table
   └─ Select "DK1" filter

2. FRONTEND (React)
   ├─ PredictionsTimeline.fetchPredictions()
   ├─ Build URL: http://localhost:3000/api/hive/predictions?limit=100&dkArea=DK1
   └─ await fetch(url)

3. BACKEND (NestJS)
   ├─ HiveController.getPredictions(limit=100, dkArea="DK1")
   ├─ HiveService.queryPredictions(100, "DK1")
   │
   ├─ Step 3a: Check Cache
   │  ├─ cacheKey = "hive:predictions:100:DK1:all"
   │  ├─ RedisService.get(cacheKey)
   │  ├─ IF cached → return cached data
   │  └─ IF NOT cached → continue to Step 3b
   │
   ├─ Step 3b: Query Hive
   │  ├─ Execute HiveQuery: "SELECT * FROM analytics.predictions LIMIT 100"
   │  ├─ Connect to HiveServer2 (localhost:10000)
   │  └─ Receive rows from HDFS via Hive
   │
   ├─ Step 3c: Filter Results
   │  ├─ Filter by dkArea == "DK1"
   │  └─ Keep only matching rows
   │
   └─ Step 3d: Cache & Return
      ├─ RedisService.set(cacheKey, result, ttl=3600)
      └─ Return JSON response

4. RESPONSE TO FRONTEND
   ├─ {
   │   "database": "analytics",
   │   "tableName": "predictions",
   │   "rows": [...],
   │   "columns": [...],
   │   "filters": {"dkArea": "DK1"}
   │ }
   └─ HTTP 200 OK

5. FRONTEND PROCESSING
   ├─ Parse JSON response
   ├─ Sort by timestamp
   ├─ Transform for Recharts
   └─ setChartData(transformed)

6. RENDER
   ├─ Draw LineChart with DK1 data only
   ├─ Show consumption (blue), price (orange), production (green)
   └─ Display statistics (avg, total, min, max)
```

### Cache Hit vs Miss Timeline

```
CACHE HIT (3rd request for same data):
─────────────────────────────────────
User Request
    │
    ▼
Redis GET cache:hive:predictions:100:DK1
    │
    ├─ ✓ FOUND (< 1ms)
    │
    ▼
Return cached data
    │
    ▼
Response to user (~10ms total)


CACHE MISS (1st request for this filter):
─────────────────────────────────────────
User Request
    │
    ▼
Redis GET cache:hive:predictions:100:DK1
    │
    ├─ ✗ NOT FOUND (< 1ms)
    │
    ▼
HiveServer2 Query
    │
    ├─ Connect (~50ms)
    ├─ Execute SELECT (~200ms)
    ├─ Filter results (~10ms)
    │
    ▼
Redis SET cache:hive:predictions:100:DK1 + TTL
    │
    ▼
Response to user (~300-400ms total)
    
Note: After this, next 3600 seconds = cache hits (~10ms)
```

---

## Key Files Summary

### Backend
| File | Purpose |
|------|---------|
| [Dashboard/backend/src/modules/hive/hive.service.ts](Dashboard/backend/src/modules/hive/hive.service.ts) | Hive query execution, caching logic, data transformation |
| [Dashboard/backend/src/modules/hive/hive.controller.ts](Dashboard/backend/src/modules/hive/hive.controller.ts) | REST API endpoints (/api/hive/*) |
| [Dashboard/backend/src/modules/redis/redis.service.ts](Dashboard/backend/src/modules/redis/redis.service.ts) | Redis connection, cache get/set/del operations |
| [Dashboard/backend/src/main.ts](Dashboard/backend/src/main.ts) | Application entry point, Swagger setup |

### Frontend
| File | Purpose |
|------|---------|
| [Dashboard/frontend/src/components/PredictionsTimeline.js](Dashboard/frontend/src/components/PredictionsTimeline.js) | Main chart component for energy predictions |
| [Dashboard/frontend/src/components/TableList.js](Dashboard/frontend/src/components/TableList.js) | Sidebar with available tables |
| [Dashboard/frontend/src/components/TableViewer.js](Dashboard/frontend/src/components/TableViewer.js) | Generic table data display |
| [Dashboard/frontend/src/pages/Dashboard.js](Dashboard/frontend/src/pages/Dashboard.js) | Main page layout/routing |
| [Dashboard/frontend/src/services/hiveService.js](Dashboard/frontend/src/services/hiveService.js) | API client for Hive queries |

### Infrastructure
| File | Purpose |
|------|---------|
| [kubernetes/hive/initial-data/data_transfer_all.sh](kubernetes/hive/initial-data/data_transfer_all.sh) | Automates Hive table creation and CSV loading to HDFS |
| [Guides/HDFS_structure.md](Guides/HDFS_structure.md) | HDFS directory creation guide |
| [Guides/Hive.md](Guides/Hive.md) | Hive deployment and table setup guide |
| [Dashboard/k8s/configmap.yaml](Dashboard/k8s/configmap.yaml) | Kubernetes configuration for services |
| [Dashboard/docker-compose.yml](Dashboard/docker-compose.yml) | Local Docker deployment |

---

## Dependencies

### Backend
```json
{
  "@nestjs/common": "^10.2.10",
  "@nestjs/config": "^3.1.1",
  "@nestjs/swagger": "^11.2.3",
  "redis": "^4.6.10",
  "axios": "^1.6.0"
}
```

### Frontend
```json
{
  "react": "^18.2.0",
  "recharts": "^2.x.x"
}
```

### Infrastructure
- Kubernetes cluster with HDFS (NameNode: namenode-g5)
- HiveServer2 (Port 10000)
- Redis (Port 6379)
- NameNode Web UI (Port 50070)

---

## Performance Metrics

### Response Times
- **Cache Hit**: ~10ms
- **Cache Miss (Hive Query)**: ~300-400ms
- **Network Latency**: ~50ms

### Cache Statistics
- **TTL**: 3600 seconds (1 hour)
- **Data Size**: Variable (30-100 rows typical)
- **Replication**: Redis in-memory

### Scalability
- **Concurrent Users**: Limited by Redis/Hive connection pooling
- **Data Volume**: HDFS distributed (replication factor 3)
- **Query Optimization**: Covered by Hive query planning

---

## Environment Setup

### Local Development
```bash
cd Dashboard/backend
npm install
npm run start:dev  # Starts on :3000

cd Dashboard/frontend
npm install
npm start          # Starts on :3001
```

### Kubernetes Deployment
```bash
cd Dashboard/k8s
./deploy.sh
```

---

## Troubleshooting

### Redis Connection Issues
```
Check: kubectl logs -l app=redis -n dashboard
Fix: Verify REDIS_HOST and REDIS_PORT in configmap.yaml
```

### Hive Query Timeout
```
Check: kubectl logs deployment/hive-server -n bd-bd-gr-05
Fix: Ensure HiveServer2 is running on port 10000
```

### HDFS Access Issues
```
Check: kubectl exec -it namenode-g5-0 -- hdfs dfs -ls /
Fix: Verify HDFS cluster health and replication
```

---

**Document Generated**: December 21, 2025
**Last Updated**: Based on current codebase analysis
