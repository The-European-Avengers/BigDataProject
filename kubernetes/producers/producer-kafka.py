import os
import time
import json
import requests
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import SerializerError
from datetime import datetime, timezone

# === Environment Variables ===
api_url = os.getenv("API_URL", "https://dmigw.govcloud.dk/v1/forecastedr/collections/harmonie_dini_sf/cube")
api_key = os.getenv("API_KEY", "YOUR_DEFAULT_KEY")
topic = os.getenv("TOPIC", "weather")
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "kafka-g5-controller-headless:9092")
schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
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
print(f"Schema Registry: {schema_registry_url}")

# === Avro Schema (will be registered with Schema Registry) ===
def get_avro_schema_str(parameter_name):
    """Returns Avro schema as JSON string"""
    return json.dumps({
        "namespace": "weather.avro",
        "type": "record",
        "name": "WeatherRecord",
        "fields": [
            {"name": "lon", "type": "double"},
            {"name": "lat", "type": "double"},
            {"name": parameter_name, "type": "double"},
            {"name": "step", "type": "string"}
        ]
    })

# Get schema string
value_schema_str = get_avro_schema_str(parameter)
print(f"Schema: {value_schema_str}")

# === Kafka AvroProducer with Schema Registry ===
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'schema.registry.url': schema_registry_url,
    # Optional: performance tuning
    'compression.type': 'snappy',
    'linger.ms': 10,
    'batch.size': 16384
}

producer = AvroProducer(
    producer_config,
    default_value_schema=value_schema_str
)

print(f"Producer started. Bootstrap: {bootstrap_servers}, Topic: {topic}, Parameter: {parameter}")

# === Delivery callback ===
def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

# === Loop to Continuously Fetch and Send ===
while True:
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        records = []
        for f in data['features']:
            lon, lat = f['geometry']['coordinates']
            props = f['properties']
            props.update({"lon": lon, "lat": lat})
            records.append(props)

        df = pd.DataFrame(records)
        payload = df.to_dict(orient="records")
        print(f"Fetched {len(payload)} records from API.")
        
        for record in payload:
            avro_record = {
                "lon": record["lon"],
                "lat": record["lat"],
                parameter: record[parameter],
                "step": record["step"]
            }
            
            try:
                # Send with schema registry integration
                producer.produce(
                    topic=topic, 
                    value=avro_record,
                    callback=delivery_report
                )
                # Trigger callbacks
                producer.poll(0)
                
            except SerializerError as e:
                print(f"Message serialization failed: {e}")
            except Exception as e:
                print(f"Error sending message: {e}")
        
        # Wait for any outstanding messages to be delivered
        producer.flush()
        print(f"Sent {len(payload)} records to topic {topic}")

    except Exception as e:
        print(f"Error fetching/sending data: {e}")

    time.sleep(poll_interval)