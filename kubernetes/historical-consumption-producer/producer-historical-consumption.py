#!/usr/bin/env python3
"""
Historical Heating Consumption Kafka Producer
Collects previous month's heating consumption data from Danish Energy Data Service API
and sends to Kafka with Avro schema.
Runs on the 2nd of each month to collect the previous month's complete data.
"""

import os
import time
import uuid
import requests
from datetime import datetime, timezone, timedelta
from calendar import monthrange

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# ===============================================================
# CONFIGURATION
# ===============================================================

# API Configuration
BASE_URL = "https://api.energidataservice.dk/dataset/PrivateConsumptionHeatingHour"

# Kafka Configuration
TOPIC = os.getenv("TOPIC", "historical-consumption")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka-g5-controller-headless:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

# Polling Configuration
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "86400"))  # Default: 24 hours (check daily)
RUN_ONCE = os.getenv("RUN_ONCE", "false").lower() == "true"
FORCE_RUN = os.getenv("FORCE_RUN", "false").lower() == "true"  # Force run regardless of date

print("=" * 80)
print("HISTORICAL HEATING CONSUMPTION KAFKA PRODUCER")
print("=" * 80)
print(f"Topic: {TOPIC}")
print(f"Bootstrap Servers: {BOOTSTRAP_SERVERS}")
print(f"Poll Interval: {POLL_INTERVAL}s ({POLL_INTERVAL/3600:.1f}h)")
print(f"Run Once Mode: {RUN_ONCE}")
print(f"Force Run: {FORCE_RUN}")
print("=" * 80)

# ===============================================================
# AVRO SCHEMA
# ===============================================================

consumption_schema = """
{
  "namespace": "consumption.avro",
  "type": "record",
  "name": "HeatingConsumptionRecord",
  "fields": [
    {"name": "TimeDK", "type": "string"},
    {"name": "TimeUTC", "type": "string"},
    {"name": "Municipality", "type": "string"},
    {"name": "MunicipalityCode", "type": "int"},
    {"name": "RegionName", "type": "string"},
    {"name": "HeatingCategory", "type": "string"},
    {"name": "HousingCategory", "type": "string"},
    {"name": "ConsumptionkWh", "type": "double"},
    {"name": "batchId", "type": "string"},
    {"name": "yearMonth", "type": "string"}
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
    schema_str=consumption_schema
)

producer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "value.serializer": avro_serializer
}

producer = SerializingProducer(producer_conf)
print(f"✓ Producer connected to {BOOTSTRAP_SERVERS}")

# ===============================================================
# CHECK IF IT'S TIME TO RUN
# ===============================================================

def should_run_today():
    """
    Check if we should run today.
    Returns True if:
    - FORCE_RUN is True, OR
    - Today is the 2nd of the month
    """
    if FORCE_RUN:
        print("\n⚠️  FORCE_RUN enabled - running regardless of date")
        return True
    
    today = datetime.now(timezone.utc)
    
    if today.day == 2:
        print(f"\n✓ Today is the 2nd of the month - time to collect previous month's data")
        return True
    else:
        print(f"\n⏭️  Today is day {today.day} of the month - skipping (only run on the 2nd)")
        return False

# ===============================================================
# CALCULATE PREVIOUS MONTH
# ===============================================================

def get_previous_month():
    """
    Calculate the previous month's year and month.
    Returns (year, month, start_date, end_date)
    """
    today = datetime.now(timezone.utc)
    
    # Calculate previous month
    if today.month == 1:
        prev_year = today.year - 1
        prev_month = 12
    else:
        prev_year = today.year
        prev_month = today.month - 1
    
    # Calculate date boundaries
    start_date = f"{prev_year}-{prev_month:02d}-01T00:00"
    
    # End date is the first day of current month
    if prev_month == 12:
        end_year = prev_year + 1
        end_month = 1
    else:
        end_year = prev_year
        end_month = prev_month + 1
    
    end_date = f"{end_year}-{end_month:02d}-01T00:00"
    
    return prev_year, prev_month, start_date, end_date

# ===============================================================
# FETCH CONSUMPTION DATA
# ===============================================================

def fetch_month_data(year, month, start_date, end_date):
    """
    Fetch all consumption data for a specific month.
    API returns data in batches, so we need to paginate.
    """
    print(f"\nFetching consumption data for {year}-{month:02d}...")
    print(f"Date range: {start_date} to {end_date}")
    
    all_records = []
    offset = 0
    limit = 20000  # API limit
    batch_num = 1
    
    while True:
        print(f"  Batch {batch_num}: offset {offset}...")
        
        params = {
            "start": start_date,
            "end": end_date,
            "timezone": "dk",
            "limit": limit,
            "offset": offset
        }
        
        try:
            response = requests.get(BASE_URL, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            records = data.get("records", [])
            
            if not records:
                print(f"  ✓ No more data - finished pagination")
                break
            
            all_records.extend(records)
            print(f"  ✓ Fetched {len(records)} records. Total: {len(all_records):,}")
            
            # If we got fewer records than the limit, we've reached the end
            if len(records) < limit:
                print(f"  ✓ Received fewer than {limit} records - finished pagination")
                break
            
            offset += limit
            batch_num += 1
            
            # Small delay to be nice to the API
            time.sleep(0.1)
            
        except requests.exceptions.RequestException as e:
            print(f"  ✗ API error: {e}")
            raise
        except Exception as e:
            print(f"  ✗ Unexpected error: {e}")
            raise
    
    print(f"\n✓ Total records fetched for {year}-{month:02d}: {len(all_records):,}")
    return all_records

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

def send_record(record, batch_id, year_month):
    """Send a single consumption record to Kafka"""
    
    # Extract and validate required fields
    time_dk = record.get("TimeDK")
    time_utc = record.get("TimeUTC")
    municipality = record.get("Municipality")
    municipality_code = record.get("MunicipalityCode")
    region_name = record.get("RegionName")
    heating_category = record.get("HeatingCategory")
    housing_category = record.get("HousingCategory")
    consumption = record.get("ConsumptionkWh")
    
    # Skip records with missing critical data
    if consumption is None or municipality is None or municipality_code is None:
        return False
    
    kafka_record = {
        "TimeDK": time_dk,
        "TimeUTC": time_utc,
        "Municipality": municipality,
        "MunicipalityCode": int(municipality_code),
        "RegionName": region_name,
        "HeatingCategory": heating_category,
        "HousingCategory": housing_category,
        "ConsumptionkWh": float(consumption),
        "batchId": batch_id,
        "yearMonth": year_month
    }
    
    producer.produce(
        topic=TOPIC,
        value=kafka_record,
        on_delivery=delivery_report
    )
    
    return True

# ===============================================================
# PROCESS AND SEND DATA
# ===============================================================

def process_and_send_data(records, batch_id, year_month, batch_size=1000):
    """Process consumption records and send to Kafka in batches"""
    total = len(records)
    sent_count = 0
    skipped_count = 0
    
    print(f"\nSending {total:,} records to Kafka (batchId: {batch_id[:8]}...)...")
    
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        
        for record in batch:
            success = send_record(record, batch_id, year_month)
            if success:
                sent_count += 1
            else:
                skipped_count += 1
        
        # Flush after each batch
        if (i + batch_size) % 10000 == 0 or (i + batch_size) >= total:
            print(f"  Sent {sent_count:,}/{total:,} records...")
            producer.flush()
            time.sleep(0.1)
    
    # Final flush
    producer.flush()
    
    print(f"\n✓ Sent: {sent_count:,} records")
    if skipped_count > 0:
        print(f"  ⚠ Skipped: {skipped_count:,} records (null values)")
    
    return sent_count, skipped_count

# ===============================================================
# MAIN COLLECTION CYCLE
# ===============================================================

def run_collection_cycle():
    """Run one complete collection cycle for previous month's consumption data"""
    
    # Check if we should run today
    if not should_run_today():
        print("⏭️  Skipping collection - not the 2nd of the month")
        return False
    
    # Generate batch ID for this collection cycle
    batch_id = str(uuid.uuid4())
    cycle_start_time = datetime.now(timezone.utc)
    
    print("\n" + "=" * 80)
    print("🆕 NEW CONSUMPTION DATA COLLECTION CYCLE")
    print("=" * 80)
    print(f"Batch ID: {batch_id}")
    print(f"Cycle Start: {cycle_start_time}")
    print("=" * 80)
    
    try:
        # Get previous month's date range
        prev_year, prev_month, start_date, end_date = get_previous_month()
        year_month = f"{prev_year}-{prev_month:02d}"
        
        print(f"\nCollecting data for previous month: {year_month}")
        
        # Fetch consumption data
        records = fetch_month_data(prev_year, prev_month, start_date, end_date)
        
        if not records:
            print("✗ No data found for previous month")
            return False
        
        # Process and send to Kafka
        sent_count, skipped_count = process_and_send_data(records, batch_id, year_month)
        
        # Summary
        cycle_end_time = datetime.now(timezone.utc)
        cycle_duration = (cycle_end_time - cycle_start_time).total_seconds()
        
        print("\n" + "=" * 80)
        print("✅ COLLECTION CYCLE COMPLETED")
        print("=" * 80)
        print(f"Batch ID: {batch_id}")
        print(f"Month: {year_month}")
        print(f"Records sent: {sent_count:,}")
        print(f"Records skipped: {skipped_count:,}")
        print(f"Duration: {cycle_duration:.1f}s ({cycle_duration/60:.1f} minutes)")
        print("=" * 80)
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error in collection cycle: {e}")
        import traceback
        traceback.print_exc()
        return False

# ===============================================================
# MAIN LOOP
# ===============================================================

def main():
    """Main execution loop"""
    
    if RUN_ONCE:
        print("\n🔄 Running in single execution mode...")
        success = run_collection_cycle()
        if success:
            print("\n✅ Single execution completed successfully. Exiting.")
        else:
            print("\n⚠️  Single execution completed (no data sent). Exiting.")
        return
    
    # Continuous mode - check daily
    print("\n🔄 Running in continuous mode (checking daily)...")
    cycle_count = 0
    
    while True:
        cycle_count += 1
        print(f"\n\n{'=' * 80}")
        print(f"DAILY CHECK #{cycle_count}")
        print('=' * 80)
        
        run_collection_cycle()
        
        print(f"\n💤 Sleeping for {POLL_INTERVAL}s ({POLL_INTERVAL/3600:.1f}h) until next check...")
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