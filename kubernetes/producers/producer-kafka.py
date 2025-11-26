import os
import time
import json
import requests
from datetime import datetime, timezone, timedelta

import geopandas as gpd
from shapely.geometry import Point

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# ===============================================================
# CONFIG
# ===============================================================

api_url = os.getenv("API_URL", "https://dmigw.govcloud.dk/v1/forecastedr/collections/harmonie_dini_sf/cube")
api_key = os.getenv("API_KEY", "YOUR_DEFAULT_KEY")
topic = os.getenv("TOPIC", "weather")
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "kafka-g5:9092")
poll_interval = int(os.getenv("POLL_INTERVAL", "120"))
parameter = os.getenv("PARAMETER_NAME", "wind-speed-10m")
shapefile_path = os.getenv("SHAPEFILE_PATH", "./dk.shp")

print(f"Starting producer for parameter: {parameter}")


# ===============================================================
# LOAD DENMARK SHAPEFILE
# ===============================================================

print(f"Loading Denmark shapefile from: {shapefile_path}")

# Check if shapefile exists
if not os.path.exists(shapefile_path):
    print(f"✗ ERROR: Shapefile not found at {shapefile_path}")
    print(f"  Current working directory: {os.getcwd()}")
    print(f"  Please ensure the following files are present:")
    print(f"    - {shapefile_path}")
    print(f"    - {shapefile_path.replace('.shp', '.shx')}")
    print(f"    - {shapefile_path.replace('.shp', '.dbf')}")
    print(f"    - {shapefile_path.replace('.shp', '.prj')}")
    print(f"\n  Set SHAPEFILE_PATH environment variable to the correct path.")
    exit(1)

dk_shape = gpd.read_file(shapefile_path)
dk_shape = dk_shape.to_crs("EPSG:4326")  # Ensure CRS is WGS84
dk_boundary = dk_shape.union_all()  # Updated to use union_all() instead of unary_union
print(f"✓ Shapefile loaded successfully from {shapefile_path}")


# ===============================================================
# AVRO SCHEMA
# ===============================================================

weather_schema = """
{
  "namespace": "weather.avro",
  "type": "record",
  "name": "WeatherRecord",
  "fields": [
    {"name": "lon", "type": "double"},
    {"name": "lat", "type": "double"},
    {"name": "value", "type": "double"},
    {"name": "step", "type": "string"},
    {"name": "parameter", "type": "string", "default": "unknown"}
  ]
}
"""


# ===============================================================
# SCHEMA REGISTRY + PRODUCER
# ===============================================================

schema_registry_conf = {"url": os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")}
schema_registry = SchemaRegistryClient(schema_registry_conf)

avro_serializer = AvroSerializer(
    schema_registry_client=schema_registry,
    schema_str=weather_schema
)

producer_conf = {
    "bootstrap.servers": bootstrap_servers,
    "value.serializer": avro_serializer
}

producer = SerializingProducer(producer_conf)

print("Producer connected:", bootstrap_servers, " topic:", topic, " parameter:", parameter)


# ===============================================================
# API FETCH WITH RETRIES
# ===============================================================

def fetch_with_retry(url, params, max_retries=3):
    for attempt in range(max_retries):
        try:
            print(f"Fetching data (attempt {attempt + 1}/{max_retries})...")
            print(f"URL: {url}")
            print(f"Params: {params}")
            
            session = requests.Session()
            
            print("Connecting to API...")
            start_time = time.time()
            
            response = session.get(
                url,
                params=params,
                timeout=(30, 600),
                stream=True
            )
            
            connect_time = time.time() - start_time
            print(f"Connected in {connect_time:.2f}s. Status: {response.status_code}")
            
            response.raise_for_status()
            
            print("Reading response...")
            content = b''
            chunk_count = 0
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    content += chunk
                    chunk_count += 1
                    if chunk_count % 10 == 0:
                        print(f"  Downloaded {len(content) / 1024 / 1024:.2f} MB...")
            
            total_time = time.time() - start_time
            print(f"Download complete in {total_time:.2f}s. Total size: {len(content) / 1024 / 1024:.2f} MB")
            
            print("Parsing JSON...")
            data = json.loads(content)
            print(f"JSON parsed successfully. Features: {len(data.get('features', []))}")
            return data

        except Exception as e:
            print(f"Error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 30
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise


# ===============================================================
# FILTER FEATURES BY DENMARK BOUNDARY
# ===============================================================

def filter_features_by_denmark(features):
    """Filter features to only include points within Denmark"""
    print("Filtering points within Denmark...")
    filtered = []
    
    for f in features:
        lon, lat = f["geometry"]["coordinates"]
        point = Point(lon, lat)
        
        if dk_boundary.contains(point):
            filtered.append(f)
    
    print(f"✓ Filtered: {len(filtered)} points inside Denmark (from {len(features)} total)")
    return filtered


# ===============================================================
# DELIVERY CALLBACK
# ===============================================================

def delivery_report(err, msg):
    """Callback called once message is delivered or fails"""
    if err is not None:
        print(f'Message delivery failed: {err}')


# ===============================================================
# SEND RECORD TO KAFKA (AVRO)
# ===============================================================

def send_record(lon, lat, value, step, parameter_name):
    record = {
        "lon": lon,
        "lat": lat,
        "value": value,
        "step": step,
        "parameter": parameter_name
    }
    producer.produce(
        topic=topic, 
        value=record,
        on_delivery=delivery_report
    )


# ===============================================================
# SEND RECORDS IN BATCHES (Memory Efficient)
# ===============================================================

def process_features_in_batches(features, batch_size=10000):
    """Process features in batches to avoid memory issues"""
    total = len(features)
    sent_count = 0
    
    for i in range(0, total, batch_size):
        batch = features[i:i + batch_size]
        
        for f in batch:
            lon, lat = f["geometry"]["coordinates"]
            props = f["properties"]
            
            send_record(
                lon=lon,
                lat=lat,
                value=props[parameter],
                step=props["step"],
                parameter_name=parameter
            )
            sent_count += 1
        
        # Flush after each batch
        print(f"Sent {sent_count}/{total} records...")
        producer.flush()
        
        # Small delay to avoid overwhelming Kafka
        time.sleep(0.1)
    
    return sent_count


# ===============================================================
# FETCH AND PROCESS SINGLE DAY
# ===============================================================

def fetch_and_send_day(day_offset):
    """Fetch data for a single day, filter it, and send to Kafka"""
    today = datetime.now(timezone.utc).date()
    target_date = today + timedelta(days=day_offset)
    day_name = ["today", "tomorrow", "day after tomorrow"][day_offset] if day_offset < 3 else f"day +{day_offset}"
    
    # Set datetime range for the specific day
    datetime_param = f"{target_date}T00:00:00Z/{target_date}T23:59:59Z"
    
    params = {
        "bbox": "7.0,54.5,16.0,58.0",
        "parameter-name": parameter,
        "datetime": datetime_param,
        "crs": "crs84",
        "f": "GeoJSON",
        "api-key": api_key
    }
    
    print(f"\n{'='*60}")
    print(f"Processing {day_name} ({target_date})")
    print(f"{'='*60}")
    
    try:
        # Fetch data
        data = fetch_with_retry(api_url, params)
        features = data.get("features", [])
        print(f"✓ Fetched {len(features)} features for {day_name}")
        
        # Filter by Denmark boundary
        filtered_features = filter_features_by_denmark(features)
        
        if len(filtered_features) == 0:
            print(f"⚠ No data points inside Denmark for {day_name}")
            return 0
        
        # Send to Kafka
        print(f"Sending {day_name} data to Kafka...")
        sent_count = process_features_in_batches(filtered_features, batch_size=10000)
        print(f"✓ Successfully sent {sent_count} records for {day_name}")
        
        # Clear from memory
        del data
        del features
        del filtered_features
        
        return sent_count
        
    except Exception as e:
        print(f"✗ Failed to process {day_name}: {e}")
        import traceback
        traceback.print_exc()
        return 0


# ===============================================================
# MAIN LOOP
# ===============================================================

while True:
    try:
        print("\n" + "="*60)
        print(f"Starting fetch cycle: {datetime.now()}")
        print(f"Parameter: {parameter}")
        print(f"Topic: {topic}")
        print("="*60)

        total_sent = 0
        
        # Process each day sequentially: fetch -> filter -> send
        for day_offset in range(3):  # 0=today, 1=tomorrow, 2=day after tomorrow
            sent_count = fetch_and_send_day(day_offset)
            total_sent += sent_count
        
        print(f"\n{'='*60}")
        print(f"✓ Cycle complete: {total_sent} total records sent to Kafka")
        print(f"{'='*60}")

    except Exception as e:
        print(f"\n✗ Error in main loop: {e}")
        import traceback
        traceback.print_exc()

    print(f"\nSleeping for {poll_interval} seconds...")
    time.sleep(poll_interval)