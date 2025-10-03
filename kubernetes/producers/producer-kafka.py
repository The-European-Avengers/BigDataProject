import os
import time
import json
import requests
from kafka import KafkaProducer

API_URL = os.getenv("API_URL", "https://akabab.github.io/superhero-api/api/id/208.json")
TOPIC = os.getenv("TOPIC", "superheroes")
BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "kafka-g5:9092")
POLL_SECONDS = int(os.getenv("POLL_INTERVAL", "10"))

def make_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )

def fetch_data():
    try:
        r = requests.get(API_URL, timeout=10)
        if r.status_code == 200:
            return r.json()
        else:
            print("API responded", r.status_code, r.text)
            return None
    except Exception as e:
        print("Error calling API:", e)
        return None

def main():
    p = make_producer()
    print("Producer started. bootstrap:", BOOTSTRAP, " topic:", TOPIC)
    while True:
        data = fetch_data()
        if data is not None:
            try:
                p.send(TOPIC, data)
                p.flush()
                print("Sent:", data.get("name", ""), " (id:", data.get("id", ""))  
            except Exception as e:
                print("Error sent to Kafka:", e)
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
