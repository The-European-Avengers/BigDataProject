import os
import time
import json
import requests
from datetime import datetime, timezone

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

today = datetime.now(timezone.utc).date()
datetime_param = f"{today}T00:00:00Z/.."

params = {
    "bbox": "7.0,54.5,16.0,58.0",
    "parameter-name": parameter,
    "datetime": datetime_param,
    "crs": "crs84",
    "f": "GeoJSON",
    "api-key": api_key
}

print(f"Starting producer for parameter: {parameter}")


# ===============================================================
# AVRO SCHEMA (with default value for backward compatibility)
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
            session = requests.Session()
            response = session.get(
                url,
                params=params,
                timeout=(30, 600)  # connect timeout, read timeout
            )
            response.raise_for_status()
            
            print(f"Response received. Status: {response.status_code}")
            print(f"Content length: {len(response.content)} bytes ({len(response.content) / 1024 / 1024:.2f} MB)")
            print("Parsing JSON...")
            
            data = response.json()
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
# MAIN LOOP
# ===============================================================

while True:
    try:
        print("\n" + "="*60)
        print(f"Starting fetch cycle: {datetime.now()}")
        print(f"Parameter: {parameter}")
        print(f"Topic: {topic}")
        print("="*60)

        data = fetch_with_retry(api_url, params)
        features = data.get("features", [])
        print(f"Fetched {len(features)} features from API")

        # Process and send records
        print("Processing and sending records to Kafka...")
        sent_count = 0
        
        for idx, f in enumerate(features):
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

            # Progress update and periodic flush every 50k records
            if sent_count % 50000 == 0:
                print(f"Sent {sent_count}/{len(features)} records...")
                producer.flush()

        # Final flush
        print("Flushing remaining messages...")
        producer.flush()
        
        print(f"✓ Successfully sent {len(features)} records to Kafka topic '{topic}'")

    except Exception as e:
        print(f"✗ Error in main loop: {e}")
        import traceback
        traceback.print_exc()

    print(f"\nSleeping for {poll_interval} seconds...")
    time.sleep(poll_interval)