import os
import time
import json
import uuid
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
# AVRO SCHEMA (WITH forecastId)
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
    {"name": "parameter", "type": "string", "default": "unknown"},
    {"name": "forecastId", "type": "string"}
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
            for chunk in response.iter_content(chunk_size=1024 * 1024):
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
# DELIVERY CALLBACK
# ===============================================================

def delivery_report(err, msg):
    """Callback called once message is delivered or fails"""
    if err is not None:
        print(f'Message delivery failed: {err}')


# ===============================================================
# SEND RECORD TO KAFKA (AVRO) WITH forecastId
# ===============================================================

def send_record(lon, lat, value, step, parameter_name, forecast_id):
    record = {
        "lon": lon,
        "lat": lat,
        "value": value,
        "step": step,
        "parameter": parameter_name,
        "forecastId": forecast_id  # NEW: UUID for this forecast cycle
    }
    producer.produce(
        topic=topic,
        value=record,
        on_delivery=delivery_report
    )


# ===============================================================
# SEND RECORDS IN BATCHES (Memory Efficient)
# ===============================================================

def process_features_in_batches(features, forecast_id, batch_size=10000):
    """Process features in batches with the same forecastId"""
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
                parameter_name=parameter,
                forecast_id=forecast_id  # Same UUID for all records in this cycle
            )
            sent_count += 1

        # Flush after each batch
        print(f"Sent {sent_count}/{total} records...")
        producer.flush()

        # Small delay to avoid overwhelming Kafka
        time.sleep(0.1)

    return sent_count


# ===============================================================
# MAIN LOOP (WITH UUID GENERATION)
# ===============================================================

while True:
    try:
        # Generate NEW UUID for this forecast cycle
        forecast_id = str(uuid.uuid4())
        cycle_start_time = datetime.now()

        print("\n" + "=" * 80)
        print(f"🆕 NEW FORECAST CYCLE STARTING")
        print("=" * 80)
        print(f"Forecast ID: {forecast_id}")
        print(f"Cycle Start: {cycle_start_time}")
        print(f"Parameter: {parameter}")
        print(f"Topic: {topic}")
        print("=" * 80)

        data = fetch_with_retry(api_url, params)
        features = data.get("features", [])
        print(f"Fetched {len(features)} features from API")

        # Process in batches - ALL with same forecastId
        print(f"Processing and sending records to Kafka (forecastId: {forecast_id[:8]}...)...")
        sent_count = process_features_in_batches(features, forecast_id, batch_size=10000)

        cycle_end_time = datetime.now()
        cycle_duration = (cycle_end_time - cycle_start_time).total_seconds()

        print("\n" + "=" * 80)
        print(f"✅ FORECAST CYCLE COMPLETED")
        print("=" * 80)
        print(f"Forecast ID: {forecast_id}")
        print(f"Records sent: {sent_count:,}")
        print(f"Duration: {cycle_duration:.1f}s")
        print(f"Next cycle in: {poll_interval}s ({poll_interval / 3600:.1f}h)")
        print("=" * 80)

        # Clear features from memory
        del features
        del data

    except Exception as e:
        print(f"✗ Error in main loop: {e}")
        import traceback

        traceback.print_exc()

    print(f"\n💤 Sleeping for {poll_interval} seconds...")
    time.sleep(poll_interval)