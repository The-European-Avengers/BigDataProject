import os
import time
import json
import requests
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime, timezone
from fastavro import schemaless_writer
import io

# === Environment Variables ===
api_url = os.getenv("API_URL", "https://dmigw.govcloud.dk/v1/forecastedr/collections/harmonie_dini_sf/cube")
api_key = os.getenv("API_KEY", "YOUR_DEFAULT_KEY")
topic = os.getenv("TOPIC", "weather")
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "kafka-g5:9092")
poll_interval = int(os.getenv("POLL_INTERVAL", "120"))
parameter = os.getenv("PARAMETER_NAME", "wind-speed-10m")

# Current date in UTC
today = datetime.now(timezone.utc).date()
datetime_param = f"{today}T00:00:00Z/.."

# === API Parameters ===
params = {
    "bbox": "7.0,54.5,16.0,58.0",
    "parameter-name": parameter,
    "datetime": datetime_param,
    "crs": "crs84",
    "f": "GeoJSON",
    "api-key": api_key
}

print(f"Starting producer for parameter: {parameter}")

# === Avro ===
def get_avro_schema(parameter_name):
    return {
        "namespace": "weather.avro",
        "type": "record",
        "name": "WeatherRecord",
        "fields": [
            {"name": "lon", "type": "double"},
            {"name": "lat", "type": "double"},
            {"name": parameter_name, "type": "double"},
            {"name": "step", "type": "string"}
        ]
    }

def avro_serializer(record, schema):
    bytes_writer = io.BytesIO()
    schemaless_writer(bytes_writer, schema, record)
    return bytes_writer.getvalue()

avro_schema = get_avro_schema(parameter)

# === Kafka Producer ===
producer = KafkaProducer(
    bootstrap_servers=[bootstrap_servers],
    value_serializer=lambda v: avro_serializer(v, avro_schema)
)

print("Producer started. bootstrap:", bootstrap_servers, " topic:", topic, " parameter:", parameter)

def fetch_with_retry(url, params, max_retries=3):
    """Fetch data with retry logic and extended timeout"""
    for attempt in range(max_retries):
        try:
            print(f"Fetching data (attempt {attempt + 1}/{max_retries})...")
            
            # Create a session for better connection handling
            session = requests.Session()
            
            # Set longer timeout: (connect timeout, read timeout)
            response = session.get(
                url, 
                params=params, 
                timeout=(30, 600),  # 30s to connect, 600s (10 min) to read
                stream=False  # Don't stream for now, just increase timeout
            )
            
            response.raise_for_status()
            
            print(f"Response received. Status: {response.status_code}")
            print(f"Content length: {len(response.content)} bytes ({len(response.content) / 1024 / 1024:.2f} MB)")
            
            # Parse JSON
            print("Parsing JSON...")
            data = response.json()
            print(f"JSON parsed successfully. Features: {len(data.get('features', []))}")
            
            return data
            
        except requests.exceptions.Timeout as e:
            print(f"Timeout error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 30
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise
                
        except requests.exceptions.RequestException as e:
            print(f"Request error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 30
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise
                
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print(f"Error at position: {e.pos}")
            # Save problematic response for debugging
            with open(f'/tmp/bad_response_{parameter}.txt', 'wb') as f:
                f.write(response.content[:10000])  # First 10KB
            print("Saved first 10KB of response to /tmp for debugging")
            raise

# === Loop to Continuously Fetch and Send ===
while True:
    try:
        print(f"\n{'='*60}")
        print(f"Starting fetch cycle at {datetime.now()}")
        print(f"Parameter: {parameter}")
        print(f"{'='*60}")
        
        # Fetch data with retry logic
        data = fetch_with_retry(api_url, params)
        
        # Process features
        print("Processing features...")
        records = []
        for idx, f in enumerate(data['features']):
            if idx % 100000 == 0 and idx > 0:
                print(f"Processed {idx} features...")
            
            lon, lat = f['geometry']['coordinates']
            props = f['properties']
            props.update({"lon": lon, "lat": lat})
            records.append(props)
        
        print(f"Fetched {len(records)} records from API.")
        
        # Send to Kafka in batches
        print("Sending to Kafka...")
        batch_size = 1000
        total_sent = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            for record in batch:
                avro_record = {
                    "lon": record["lon"],
                    "lat": record["lat"],
                    parameter: record[parameter],
                    "step": record["step"]
                }
                producer.send(topic, avro_record)
            
            total_sent += len(batch)
            
            # Progress update every 10k records
            if total_sent % 10000 == 0:
                print(f"Sent {total_sent}/{len(records)} records...")
        
        producer.flush()
        print(f"✓ Successfully sent {len(records)} records to topic {topic}")
        
    except Exception as e:
        print(f"✗ Error in main loop: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\nSleeping for {poll_interval} seconds...")
    time.sleep(poll_interval)