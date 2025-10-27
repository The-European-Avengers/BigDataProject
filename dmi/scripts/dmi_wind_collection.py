#!/usr/bin/env python3
"""
DMI Wind Collection
Collects wind data from all Danish weather stations that have wind measurements
Clean data with no empty values - perfect for analysis and streaming
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

# All Danish weather stations with confirmed wind data
WIND_STATIONS = {
    "06154": "Brandelev",
    "06068": "Isenvad",
    "06082": "Borris",
    "06180": "Københavns Lufthavn",
    "06110": "Flyvestation Skrydstrup",
    "06120": "Odense Lufthavn",
    "06072": "Ødum",
    "06031": "Tylstrup",
    "06060": "Flyvestation Karup",
    "06181": "Jægersborg",
    "06193": "Hammer Odde Fyr",
    "06188": "Sjælsmark",
    "06096": "Rømø/Juvre",
    "06108": "Kolding Lufthavn",
    "06069": "Foulum",
    "06147": "Vindebæk Kyst",
    "06074": "Århus Syd",
    "06124": "Sydfyns Flyveplads",
    "06093": "Vester Vedsted",
    "06079": "Anholt Havn",
    "06058": "Hvide Sande",
    "06056": "Mejrup",
    "06116": "Store Jyndevad",
    "06118": "Sønderborg Lufthavn",
    "06081": "Blåvandshuk Fyr",
    "06138": "Langø",
    "06169": "Gniben",
    "06080": "Esbjerg Lufthavn",
    "06159": "Røsnæs Fyr",
    "06032": "Stenhøj",
    "06136": "Tystofte",
    "06168": "Nakkehoved Fyr",
    "06119": "Kegnæs Fyr",
    "06104": "Billund Lufthavn",
    "06041": "Skagen Fyr",
    "06151": "Omø Fyr",
    "06174": "Tessebølle",
    "06123": "Assens/Torø",
    "06013": "Klaksvik Heliport",
    "06135": "Flakkebjerg",
    "06141": "Abed",
    "06065": "Års Syd",
    "06197": "Nexø Vest",
    "06073": "Sletterhage Fyr",
    "06049": "Hald Vest",
    "06052": "Thyborøn",
    "06149": "Gedser",
    "06126": "Årslev",
    "06170": "Roskilde Lufthavn",
    "06132": "Tranebjerg Øst",
    "06156": "Holbæk",
    "06109": "Askov",
    "06070": "Århus Lufthavn",
    "06030": "Flyvestation Ålborg",
    "06102": "Horsens/Bygholm",
    "06019": "Silstrup",
    "06190": "Bornholms Lufthavn",
}

def setup_directories():
    """Create data directory if it doesn't exist."""
    if not os.path.exists("data"):
        os.makedirs("data")
        print("Created data directory")

def fetch_station_month(station_id, year, month):
    """Fetch wind data for one station for one month."""
    start_date = f"{year}-{month:02d}-01T00:00:00Z"
    if month == 12:
        end_date = f"{year+1}-01-01T00:00:00Z"
    else:
        end_date = f"{year}-{month+1:02d}-01T00:00:00Z"
    
    params = {
        "stationId": station_id,
        "parameterId": "mean_wind_speed",
        "datetime": f"{start_date}/{end_date}",
        "limit": 1000
    }
    
    headers = {"X-Gravitee-Api-Key": API_KEY}
    
    print(f"Calling API: {station_id} wind {year}-{month:02d}")
    
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

def collect_wind_data(year):
    """Collect wind data for one year from all wind stations."""
    print(f"Collecting wind data for {year}")
    print(f"Stations: {len(WIND_STATIONS)} (all have wind data)")
    
    all_data = []
    stations_with_data = 0
    
    for station_id, station_name in WIND_STATIONS.items():
        print(f"\nProcessing {station_name} ({station_id})")
        station_has_data = False
        
        for month in range(1, 13):
            features = fetch_station_month(station_id, year, month)
            
            if features:
                station_has_data = True
                for feature in features:
                    props = feature["properties"]
                    wind_value = props.get("value")
                    
                    # Only add records with actual wind data
                    if wind_value is not None:
                        all_data.append({
                            "timeObserved": props.get("from"),
                            "stationId": station_id,
                            "stationName": station_name,
                            "mean_wind_speed": wind_value
                        })
            
            time.sleep(0.1)
        
        if station_has_data:
            stations_with_data += 1
            print(f"  -> Station has data")
        else:
            print(f"  -> Station skipped - no data available")
    
    print(f"\nSummary: {stations_with_data}/{len(WIND_STATIONS)} stations have data")
    return all_data

def save_data(data, year):
    """Save collected wind data to CSV."""
    if not data:
        print(f"No data collected for {year}")
        return None
    
    df = pd.DataFrame(data)
    df["timeObserved"] = pd.to_datetime(df["timeObserved"], format='ISO8601')
    df = df.sort_values(["stationId", "timeObserved"])
    
    # Remove any rows where mean_wind_speed is null (extra safety)
    original_count = len(df)
    df = df.dropna(subset=['mean_wind_speed'])
    cleaned_count = len(df)
    
    if original_count != cleaned_count:
        print(f"Cleaned {original_count - cleaned_count} null wind records")
    
    filename = f"data/{year}_dmi_wind.csv"
    df.to_csv(filename, index=False)
    
    print(f"\nSaved {len(df)} wind records to {filename}")
    print(f"Date range: {df['timeObserved'].min()} to {df['timeObserved'].max()}")
    print(f"Stations: {df['stationId'].nunique()}")
    print(f"Average mean_wind_speed: {df['mean_wind_speed'].mean():.2f}")
    print(f"Max mean_wind_speed: {df['mean_wind_speed'].max():.2f}")
    
    return filename

def main():
    """Main function."""
    print("DMI Wind Collection")
    print(f"Target Year: {TARGET_YEAR}")
    print("All 57 Danish weather stations with wind data")
    
    setup_directories()
    
    filename = f"data/{TARGET_YEAR}_dmi_wind.csv"
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
    
    # Collect wind data
    wind_data = collect_wind_data(TARGET_YEAR)
    
    # Save to CSV
    output_file = save_data(wind_data, TARGET_YEAR)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"Total time: {duration}")
    print(f"Complete. DMI wind data saved to: {output_file}")
    print("Clean data ready for analysis and Kafka streaming!")

if __name__ == "__main__":
    main()