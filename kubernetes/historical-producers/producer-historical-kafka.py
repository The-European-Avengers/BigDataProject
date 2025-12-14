#!/usr/bin/env python3
"""
Historical Weather Data Kafka Producer
Collects historical weather data from DMI API for yesterday and sends to Kafka with Avro schema.
Configurable via environment variables for different parameters.
"""

import os
import time
import json
import uuid
import requests
from datetime import datetime, timezone, timedelta

import geopandas as gpd
from shapely.geometry import Point

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# ===============================================================
# CONFIGURATION
# ===============================================================

# DMI API Configuration
API_KEY = os.getenv("API_KEY", "d36196e2-2a58-4497-bf28-f71d18c427a1")
BASE_URL = "https://dmigw.govcloud.dk/v2/climateData/collections/stationValue/items"

# Kafka Configuration
TOPIC = os.getenv("TOPIC", "weather-historical")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka-g5:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

# Parameter Configuration
PARAMETER_ID = os.getenv("PARAMETER_ID", "mean_radiation")  # DMI API parameter: mean_radiation, mean_wind_speed, etc.
VALUE_FIELD_NAME = os.getenv("VALUE_FIELD_NAME", "mean_radiation")  # Column name in data: mean_radiation, mean_wind_speed, etc.

# Shapefile Configuration
SHAPEFILE_PATH = os.getenv("SHAPEFILE_PATH", "./dk.shp")

# Polling Configuration
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "86400"))  # Default: 24 hours
RUN_ONCE = os.getenv("RUN_ONCE", "false").lower() == "true"  # Run once and exit

print("=" * 80)
print("HISTORICAL WEATHER DATA KAFKA PRODUCER")
print("=" * 80)
print(f"Parameter ID: {PARAMETER_ID}")
print(f"Value Field Name: {VALUE_FIELD_NAME}")
print(f"Topic: {TOPIC}")
print(f"Bootstrap Servers: {BOOTSTRAP_SERVERS}")
print(f"Poll Interval: {POLL_INTERVAL}s ({POLL_INTERVAL/3600:.1f}h)")
print(f"Run Once Mode: {RUN_ONCE}")
print("=" * 80)

# ===============================================================
# LOAD DENMARK SHAPEFILE
# ===============================================================

print(f"\nLoading Denmark shapefile from: {SHAPEFILE_PATH}")

if not os.path.exists(SHAPEFILE_PATH):
    print(f"✗ ERROR: Shapefile not found at {SHAPEFILE_PATH}")
    print(f"  Current working directory: {os.getcwd()}")
    print(f"  Please ensure the shapefile and supporting files are present.")
    exit(1)

dk_shape = gpd.read_file(SHAPEFILE_PATH)
dk_shape = dk_shape.to_crs("EPSG:4326")  # Ensure CRS is WGS84
dk_boundary = dk_shape.union_all()
print(f"✓ Shapefile loaded successfully")

# ===============================================================
# AVRO SCHEMA
# ===============================================================

weather_schema = """
{
  "namespace": "weather.avro",
  "type": "record",
  "name": "HistoricalWeatherRecord",
  "fields": [
    {"name": "timeObserved", "type": "string"},
    {"name": "stationId", "type": "string"},
    {"name": "stationName", "type": "string"},
    {"name": "lon", "type": "double"},
    {"name": "lat", "type": "double"},
    {"name": "value", "type": "double"},
    {"name": "valueFieldName", "type": "string"},
    {"name": "batchId", "type": "string"}
  ]
}
"""

# ===============================================================
# SCHEMA REGISTRY + PRODUCER
# ===============================================================

schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
schema_registry = SchemaRegistryClient(schema_registry_conf)

avro_serializer = AvroSerializer(
    schema_registry_client=schema_registry,
    schema_str=weather_schema
)

producer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "value.serializer": avro_serializer
}

producer = SerializingProducer(producer_conf)
print(f"✓ Producer connected to {BOOTSTRAP_SERVERS}")

# ===============================================================
# STATION DISCOVERY AND GEOCODING
# ===============================================================

def get_stations_with_parameter(start_date: str, end_date: str) -> dict:
    """
    Discover all Danish stations that have data for the specified parameter.
    Returns dict mapping stationId -> {name, lon, lat}
    """
    print(f"\nDiscovering stations with {PARAMETER_ID}...")
    
    params = {
        "parameterId": PARAMETER_ID,
        "datetime": f"{start_date}/{end_date}",
        "limit": 300000,
    }
    headers = {"X-Gravitee-Api-Key": API_KEY}

    try:
        resp = requests.get(BASE_URL, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        
        data = resp.json()
        features = data.get("features", [])
        
        if not features:
            print(f"✗ No data found for {PARAMETER_ID}")
            return {}
        
        # Extract unique station IDs
        station_ids = sorted({f["properties"].get("stationId") for f in features if f.get("properties")})
        print(f"  Found {len(station_ids)} stations with data")
        
        # Get station metadata (name, coordinates, country)
        station_url = "https://dmigw.govcloud.dk/v2/climateData/collections/station/items"
        station_params = {"limit": 300000}
        sresp = requests.get(station_url, params=station_params, headers=headers, timeout=60)
        
        station_info = {}
        if sresp.status_code == 200:
            sdata = sresp.json()
            for feat in sdata.get("features", []):
                props = feat.get("properties", {})
                geom = feat.get("geometry", {})
                sid = props.get("stationId")
                
                # Filter to Denmark and stations we have data for
                if sid in station_ids and props.get("country") == "DNK":
                    coords = geom.get("coordinates", [None, None])
                    station_info[sid] = {
                        "name": props.get("name", "Unknown"),
                        "lon": coords[0],
                        "lat": coords[1]
                    }
        
        print(f"  ✓ {len(station_info)} Danish stations with metadata")
        return station_info
        
    except Exception as e:
        print(f"✗ Error discovering stations: {e}")
        raise

# ===============================================================
# FILTER STATIONS BY DENMARK BOUNDARY
# ===============================================================

def filter_stations_by_denmark(station_info: dict) -> dict:
    """Filter stations to only include those within Denmark boundary"""
    print("\nFiltering stations by Denmark boundary...")
    filtered = {}
    
    for sid, info in station_info.items():
        lon, lat = info["lon"], info["lat"]
        if lon is not None and lat is not None:
            point = Point(lon, lat)
            if dk_boundary.contains(point):
                filtered[sid] = info
    
    print(f"  ✓ {len(filtered)} stations inside Denmark (from {len(station_info)} total)")
    return filtered

# ===============================================================
# FETCH HISTORICAL DATA
# ===============================================================

def fetch_historical_data(start_date: str, end_date: str, station_ids: list) -> list:
    """
    Fetch historical data for all stations for the specified date range.
    Returns list of features with observations.
    """
    print(f"\nFetching historical data from {start_date} to {end_date}...")
    
    params = {
        "parameterId": PARAMETER_ID,
        "datetime": f"{start_date}/{end_date}",
        "limit": 300000,
    }
    headers = {"X-Gravitee-Api-Key": API_KEY}
    
    try:
        print(f"  Calling API...")
        resp = requests.get(BASE_URL, params=params, headers=headers, timeout=120)
        resp.raise_for_status()
        
        data = resp.json()
        features = data.get("features", [])
        
        print(f"  ✓ Fetched {len(features)} observations")
        
        # Filter to only our Danish stations
        station_set = set(station_ids)
        filtered_features = [
            f for f in features 
            if f.get("properties", {}).get("stationId") in station_set
        ]
        
        print(f"  ✓ {len(filtered_features)} observations from Danish stations")
        return filtered_features
        
    except Exception as e:
        print(f"  ✗ Error fetching data: {e}")
        raise

# ===============================================================
# DELIVERY CALLBACK
# ===============================================================

def delivery_report(err, msg):
    """Callback called once message is delivered or fails"""
    if err is not None:
        print(f'✗ Message delivery failed: {err}')

# ===============================================================
# SEND RECORD TO KAFKA
# ===============================================================

def send_record(time_observed, station_id, station_info, value, value_field_name, batch_id):
    """Send a single record to Kafka"""
    record = {
        "timeObserved": time_observed,
        "stationId": station_id,
        "stationName": station_info.get("name", "Unknown"),
        "lon": station_info.get("lon", 0.0),
        "lat": station_info.get("lat", 0.0),
        "value": value,
        "valueFieldName": value_field_name,
        "batchId": batch_id
    }
    
    producer.produce(
        topic=TOPIC,
        value=record,
        on_delivery=delivery_report
    )

# ===============================================================
# PROCESS AND SEND DATA
# ===============================================================

def process_and_send_data(features, station_info, batch_id, batch_size=1000):
    """Process features and send to Kafka in batches"""
    total = len(features)
    sent_count = 0
    skipped_count = 0
    
    print(f"\nSending {total} records to Kafka (batchId: {batch_id[:8]}...)...")
    
    for i in range(0, total, batch_size):
        batch = features[i:i + batch_size]
        
        for f in batch:
            props = f.get("properties", {})
            station_id = props.get("stationId")
            value = props.get("value")
            time_observed = props.get("from")  # ISO timestamp
            
            # Skip records with null values
            if value is None or station_id not in station_info:
                skipped_count += 1
                continue
            
            send_record(
                time_observed=time_observed,
                station_id=station_id,
                station_info=station_info[station_id],
                value=value,
                value_field_name=VALUE_FIELD_NAME,
                batch_id=batch_id
            )
            sent_count += 1
        
        # Flush after each batch
        if (i + batch_size) % 5000 == 0 or (i + batch_size) >= total:
            print(f"  Sent {sent_count}/{total} records...")
            producer.flush()
            time.sleep(0.1)
    
    # Final flush
    producer.flush()
    
    print(f"  ✓ Sent: {sent_count:,} records")
    if skipped_count > 0:
        print(f"  ⚠ Skipped: {skipped_count:,} records (null values)")
    
    return sent_count, skipped_count

# ===============================================================
# MAIN COLLECTION CYCLE
# ===============================================================

def run_collection_cycle():
    """Run one complete collection cycle for yesterday's data"""
    
    # Generate batch ID for this collection cycle
    batch_id = str(uuid.uuid4())
    cycle_start_time = datetime.now(timezone.utc)
    
    print("\n" + "=" * 80)
    print("🆕 NEW HISTORICAL DATA COLLECTION CYCLE")
    print("=" * 80)
    print(f"Batch ID: {batch_id}")
    print(f"Cycle Start: {cycle_start_time}")
    print(f"Parameter: {PARAMETER_ID} (field: {VALUE_FIELD_NAME})")
    print("=" * 80)
    
    # Calculate yesterday's date range
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    start_date = f"{yesterday}T00:00:00Z"
    end_date = f"{yesterday}T23:59:59Z"
    
    print(f"\nCollecting data for: {yesterday}")
    print(f"Date range: {start_date} to {end_date}")
    
    try:
        # Step 1: Discover stations
        station_info = get_stations_with_parameter(start_date, end_date)
        
        if not station_info:
            print("✗ No stations found. Skipping cycle.")
            return
        
        # Step 2: Filter stations by Denmark boundary
        station_info = filter_stations_by_denmark(station_info)
        
        if not station_info:
            print("✗ No Danish stations found. Skipping cycle.")
            return
        
        # Step 3: Fetch historical data
        features = fetch_historical_data(start_date, end_date, list(station_info.keys()))
        
        if not features:
            print("✗ No data found. Skipping cycle.")
            return
        
        # Step 4: Process and send to Kafka
        sent_count, skipped_count = process_and_send_data(features, station_info, batch_id)
        
        # Summary
        cycle_end_time = datetime.now(timezone.utc)
        cycle_duration = (cycle_end_time - cycle_start_time).total_seconds()
        
        print("\n" + "=" * 80)
        print("✅ COLLECTION CYCLE COMPLETED")
        print("=" * 80)
        print(f"Batch ID: {batch_id}")
        print(f"Date: {yesterday}")
        print(f"Stations: {len(station_info)}")
        print(f"Records sent: {sent_count:,}")
        print(f"Records skipped: {skipped_count:,}")
        print(f"Duration: {cycle_duration:.1f}s")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n✗ Error in collection cycle: {e}")
        import traceback
        traceback.print_exc()

# ===============================================================
# MAIN LOOP
# ===============================================================

def main():
    """Main execution loop"""
    
    if RUN_ONCE:
        print("\n🔄 Running in single execution mode...")
        run_collection_cycle()
        print("\n✅ Single execution completed. Exiting.")
        return
    
    # Continuous mode
    print("\n🔄 Running in continuous mode...")
    cycle_count = 0
    
    while True:
        cycle_count += 1
        print(f"\n\n{'=' * 80}")
        print(f"CYCLE #{cycle_count}")
        print('=' * 80)
        
        run_collection_cycle()
        
        print(f"\n💤 Sleeping for {POLL_INTERVAL}s ({POLL_INTERVAL/3600:.1f}h) until next cycle...")
        time.sleep(POLL_INTERVAL)

# ===============================================================
# ENTRY POINT
# ===============================================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user. Shutting down...")
        producer.flush()
        print("✓ Producer flushed. Goodbye!")
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)