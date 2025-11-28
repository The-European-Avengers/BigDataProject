#!/usr/bin/env python3
"""
Debug script to check Kafka topics and Schema Registry
"""
import requests
import os
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer

# Configuration
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka-g5:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")


def check_topics():
    """Check if input topics exist and have data"""
    print("\n" + "=" * 60)
    print("CHECKING KAFKA TOPICS")
    print("=" * 60)

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=10)

    input_topics = ["weather-wind", "weather-temp", "weather-sun"]
    output_topics = ["weather-wind-enriched-art", "weather-temp-enriched-art", "weather-sun-enriched-art"]

    print("\nInput Topics:")
    for topic in input_topics:
        if topic in metadata.topics:
            partitions = len(metadata.topics[topic].partitions)
            print(f"  ✅ {topic} - {partitions} partitions")

            # Check message count
            consumer = Consumer({
                'bootstrap.servers': BOOTSTRAP_SERVERS,
                'group.id': 'debug-group',
                'auto.offset.reset': 'earliest'
            })

            partitions_to_check = [topic_partition for topic_partition in
                                   consumer.list_topics(topic).topics[topic].partitions]
            low, high = {}, {}

            for partition in partitions_to_check:
                from confluent_kafka import TopicPartition
                tp = TopicPartition(topic, partition)
                low_offset, high_offset = consumer.get_watermark_offsets(tp, timeout=10)
                messages_in_partition = high_offset - low_offset
                print(f"      Partition {partition}: {messages_in_partition} messages")

            consumer.close()
        else:
            print(f"  ❌ {topic} - NOT FOUND")

    print("\nOutput Topics:")
    for topic in output_topics:
        if topic in metadata.topics:
            partitions = len(metadata.topics[topic].partitions)
            print(f"  ✅ {topic} - {partitions} partitions")
        else:
            print(f"  ❌ {topic} - NOT FOUND (this is expected if enricher hasn't run)")


def check_schemas():
    """Check Schema Registry for registered schemas"""
    print("\n" + "=" * 60)
    print("CHECKING SCHEMA REGISTRY")
    print("=" * 60)

    try:
        # List all subjects
        response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
        response.raise_for_status()
        subjects = response.json()

        print(f"\nRegistered subjects: {len(subjects)}")
        for subject in sorted(subjects):
            # Get latest version
            version_response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest")
            if version_response.status_code == 200:
                version_data = version_response.json()
                print(f"  ✅ {subject} - version {version_data.get('version')}")
            else:
                print(f"  ⚠️  {subject} - could not fetch version")

    except Exception as e:
        print(f"  ❌ Error connecting to Schema Registry: {e}")


def check_consumer_group():
    """Check if consumer group exists and its lag"""
    print("\n" + "=" * 60)
    print("CHECKING CONSUMER GROUPS")
    print("=" * 60)

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    groups = admin.list_consumer_groups(timeout=10)

    print("\nConsumer Groups:")
    for group in groups.valid:
        print(f"  • {group.group_id}")


if __name__ == "__main__":
    print("🔍 Kafka & Schema Registry Diagnostic Tool")
    check_topics()
    check_schemas()
    check_consumer_group()
    print("\n" + "=" * 60)
    print("Diagnostic complete!")
    print("=" * 60)