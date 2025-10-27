import os
import time
import json
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
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
today = datetime.now(timezone.utc).date()  # e.g., 2025-10-25

# Format as ISO 8601 string for the API
datetime_param = f"{today}T00:00:00Z/.."  # 2025-10-25T00:00:00Z/..


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

avro_schema = get_avro_schema(parameter)  # parameter = wind-speed-10m, temperature-2m or direct-solar-exposure

# === Kafka Producer ===
producer = KafkaProducer(
    bootstrap_servers=[bootstrap_servers],
    value_serializer=lambda v: avro_serializer(v, avro_schema)
)

print("Producer started. bootstrap:", bootstrap_servers, " topic:", topic, " parameter:", parameter)

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
            producer.send(topic, avro_record)
            #print(f"Sent record to topic {topic}: {avro_record}")
        producer.flush()
        print(f"Sent {len(payload)} records to topic {topic}")

    except Exception as e:
        print(f"Error fetching/sending data: {e}")

    time.sleep(poll_interval)
