#!/usr/bin/env python3
"""
DMI Sunlight Collection
Collects sunshine data from all Danish weather stations that have sunlight measurements
Clean data with no empty values 
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime

# CONFIGURATION
TARGET_YEAR = 2020

API_KEY = "d36196e2-2a58-4497-bf28-f71d18c427a1"
BASE_URL = "https://dmigw.govcloud.dk/v2/climateData/collections/stationValue/items"

SUNSHINE_STATIONS = {
    "06019": "Silstrup",
    "06031": "Tylstrup",
    "06041": "Skagen Fyr",
    "06056": "Mejrup",
    "06058": "Hvide Sande",
    "06065": "Års Syd",
    "06068": "Isenvad",
    "06069": "Foulum",
    "06072": "Ødum",
    "06079": "Anholt Havn",
    "06082": "Borris",
    "06096": "Rømø/Juvre",
    "06102": "Horsens/Bygholm",
    "06109": "Askov",
    "06116": "Store Jyndevad",
    "06123": "Assens/Torø",
    "06126": "Årslev",
    "06132": "Tranebjerg Øst",
    "06135": "Flakkebjerg",
    "06136": "Tystofte",
    "06141": "Abed",
    "06149": "Gedser",
    "06156": "Holbæk",
    "06174": "Tessebølle",
    "06187": "Københavns Toldbod",
    "06188": "Sjælsmark",
    "06193": "Hammer Odde Fyr",
    "06197": "Nexø Vest",
}

def setup_directories():
    """Create data directory if it doesn't exist."""
    if not os.path.exists("data"):
        os.makedirs("data")
        print("Created data directory")

def fetch_station_month(station_id, year, month):
    """Fetch sunshine data for one station for one month."""
    start_date = f"{year}-{month:02d}-01T00:00:00Z"
    if month == 12:
        end_date = f"{year+1}-01-01T00:00:00Z"
    else:
        end_date = f"{year}-{month+1:02d}-01T00:00:00Z"
    
    params = {
        "stationId": station_id,
        "parameterId": "bright_sunshine",
        "datetime": f"{start_date}/{end_date}",
        "limit": 1000
    }
    
    headers = {"X-Gravitee-Api-Key": API_KEY}
    
    print(f"Calling API: {station_id} sunshine {year}-{month:02d}")
    
    try:
        response = requests.get(BASE_URL, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            features = data.get("features", [])
            if len(features) > 0:
                print(f"  -> {len(features)} records")
            else:
                print(f"  -> No data available")
            return features
        else:
            print(f"  -> API Error {response.status_code}")
            return []
    except Exception as e:
        print(f"  -> Network error: {e}")
        return []

def collect_sunshine_data(year):
    """Collect sunshine data for one year from all sunshine stations."""
    print(f"Collecting sunshine data for {year}")
    print(f"Stations: {len(SUNSHINE_STATIONS)} (all have sunshine data)")
    
    all_data = []
    stations_with_data = 0
    
    for station_id, station_name in SUNSHINE_STATIONS.items():
        print(f"\nProcessing {station_name} ({station_id})")
        station_has_data = False
        
        for month in range(1, 13):
            features = fetch_station_month(station_id, year, month)
            
            if features:
                station_has_data = True
                for feature in features:
                    props = feature["properties"]
                    sunshine_value = props.get("value")
                    
                    # Only add records with actual sunshine data
                    if sunshine_value is not None:
                        all_data.append({
                            "timeObserved": props.get("from"),
                            "stationId": station_id,
                            "stationName": station_name,
                            "bright_sunshine": sunshine_value
                        })
            
            time.sleep(0.1)
        
        if station_has_data:
            stations_with_data += 1
            print(f"  -> Station has data")
        else:
            print(f"  -> Station skipped - no data available")
    
    print(f"\nSummary: {stations_with_data}/{len(SUNSHINE_STATIONS)} stations have data")
    return all_data

def save_data(data, year):
    """Save collected sunshine data to CSV."""
    if not data:
        print(f"No data collected for {year}")
        return None
    
    df = pd.DataFrame(data)
    df["timeObserved"] = pd.to_datetime(df["timeObserved"], format='ISO8601')
    df = df.sort_values(["stationId", "timeObserved"])
    
    # Remove any rows where bright_sunshine is null (extra safety)
    original_count = len(df)
    df = df.dropna(subset=['bright_sunshine'])
    cleaned_count = len(df)
    
    if original_count != cleaned_count:
        print(f"Cleaned {original_count - cleaned_count} null sunshine records")
    
    filename = f"data/{year}_dmi_sunshine.csv"
    df.to_csv(filename, index=False)
    
    print(f"\nSaved {len(df)} sunshine records to {filename}")
    print(f"Date range: {df['timeObserved'].min()} to {df['timeObserved'].max()}")
    print(f"Stations: {df['stationId'].nunique()}")
    print(f"Average bright_sunshine: {df['bright_sunshine'].mean():.2f}")
    print(f"Max bright_sunshine: {df['bright_sunshine'].max():.2f}")
    
    return filename

def main():
    """Main function."""
    print("DMI Sunlight Collection")
    print(f"Target Year: {TARGET_YEAR}")
    print("All 28 Danish weather stations with sunshine data")
    
    setup_directories()
    
    filename = f"data/{TARGET_YEAR}_dmi_sunshine.csv"
    if os.path.exists(filename):
        print(f"File {filename} already exists!")
        response = input("Overwrite? (y/n): ")
        if response.lower() != 'y':
            print("Cancelled.")
            return
        else:
            os.remove(filename)
            print("Removed existing file.")
    
    start_time = datetime.now()
    
    # Collect sunshine data
    sunshine_data = collect_sunshine_data(TARGET_YEAR)
    
    # Save to CSV
    output_file = save_data(sunshine_data, TARGET_YEAR)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"Total time: {duration}")
    print(f"Complete. DMI sunshine data saved to: {output_file}")
    print("Clean data ready for analysis and Kafka streaming!")

if __name__ == "__main__":
    main()