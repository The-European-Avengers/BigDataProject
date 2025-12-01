#!/usr/bin/env python3
"""
DMI Sunlight/Radiation Collection
Collects solar radiation data (mean_radiation) from all Danish weather stations that have radiation measurements.
Clean data with no empty values.
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime

# CONFIGURATION
TARGET_YEARS = [2020, 2021, 2022, 2023, 2024]  # Years to collect data for

# RUTA COMPARTIDA: Debe coincidir con el 'mountPath' en el contenedor HIVE y Collector (YAML)
SHARED_DATA_PATH = "/shared-data-for-hive" 

API_KEY = "d36196e2-2a58-4497-bf28-f71d18c427a1"
BASE_URL = "https://dmigw.govcloud.dk/v2/climateData/collections/stationValue/items"

def get_mean_radiation_stations(year: int) -> dict:
    """Discover all stations that have mean_radiation observations for the given year,
    filtered to Denmark (country == 'DNK').

    Returns a dict mapping stationId -> stationName (name may be 'Unknown' if lookup fails).
    """
    start_date = f"{year}-01-01T00:00:00Z"
    end_date = f"{year+1}-01-01T00:00:00Z"

    params = {
        "parameterId": "mean_radiation",
        "datetime": f"{start_date}/{end_date}",
        "limit": 300000,
    }
    headers = {"X-Gravitee-Api-Key": API_KEY}

    try:
        resp = requests.get(BASE_URL, params=params, headers=headers)
        if resp.status_code != 200:
            raise RuntimeError(f"Station discovery failed with HTTP {resp.status_code}")
        data = resp.json()
        features = data.get("features", [])
        station_ids = sorted({f["properties"].get("stationId") for f in features if f.get("properties")})
        if not station_ids:
            raise RuntimeError("No stations found for mean_radiation in selected period")

        # Try to map ids to names via station catalog and filter to Denmark (DNK)
        station_url = "https://dmigw.govcloud.dk/v2/climateData/collections/station/items"
        # Request all, we'll filter locally; API supports large limits
        station_params = {"limit": 300000}
        sresp = requests.get(station_url, params=station_params, headers=headers)
        name_by_id = {}
        if sresp.status_code == 200:
            sdata = sresp.json()
            for feat in sdata.get("features", []):
                props = feat.get("properties", {})
                sid = props.get("stationId")
                if sid in station_ids and props.get("country") == "DNK":
                    name_by_id[sid] = props.get("name") or "Unknown"
        else:
            print(f"Note: station name lookup failed ({sresp.status_code}); proceeding with Unknown names")

        # Filter station_ids down to DNK only
        filtered_ids = sorted(name_by_id.keys())
        if not filtered_ids:
            raise RuntimeError("No Denmark (DNK) stations found for mean_radiation in selected period")

        return {sid: name_by_id.get(sid, "Unknown") for sid in filtered_ids}
    except Exception as e:
        # Bubble up to caller; they can decide how to handle
        raise

def setup_directories():
    """Create shared data directory if it doesn't exist."""
    if not os.path.exists(SHARED_DATA_PATH):
        os.makedirs(SHARED_DATA_PATH)
        print(f"Created shared data directory: {SHARED_DATA_PATH}")

def fetch_station_month(station_id, year, month):
    """Fetch mean sun data for one station for one month."""
    start_date = f"{year}-{month:02d}-01T00:00:00Z"
    if month == 12:
        end_date = f"{year+1}-01-01T00:00:00Z"
    else:
        end_date = f"{year}-{month+1:02d}-01T00:00:00Z"
    
    params = {
        "stationId": station_id,
        "parameterId": "mean_radiation",
        "datetime": f"{start_date}/{end_date}",
        "limit": 1000
    }
    
    headers = {"X-Gravitee-Api-Key": API_KEY}
    
    # print(f"Calling API: {station_id} mean_radiation {year}-{month:02d}")
    
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

def collect_radiation_data(year):
    """Collect mean radiation data for one year from all radiation-capable stations."""
    print(f"Collecting mean_radiation data for {year}")
    stations = get_mean_radiation_stations(year)
    print(f"Stations discovered with mean_radiation in {year}: {len(stations)}")
    if not stations:
        raise RuntimeError("No stations discovered for mean_radiation; aborting.")
    
    all_data = []
    stations_with_data = 0
    
    # Determine how many months to fetch for the given year (limit to current month for the current year)
    now = datetime.utcnow()
    last_month = 12 if year < now.year else now.month

    for station_id, station_name in stations.items():
        print(f"\nProcessing {station_name} ({station_id})")
        station_has_data = False

        for month in range(1, last_month + 1):
            features = fetch_station_month(station_id, year, month)

            if features:
                station_has_data = True
                for feature in features:
                    props = feature["properties"]
                    radiation_value = props.get("value")

                    # Only add records with actual radiation data
                    if radiation_value is not None:
                        all_data.append({
                            "timeObserved": props.get("from"),
                            "stationId": station_id,
                            "stationName": station_name,
                            "mean_radiation": radiation_value
                        })

            time.sleep(0.1)
        
        if station_has_data:
            stations_with_data += 1
            print(f"  -> Station has data")
        else:
            print(f"  -> Station skipped - no data available")
    
    print(f"\nSummary: {stations_with_data}/{len(stations)} stations have data")
    return all_data

def save_data(data, year):
    """Save collected mean radiation data to CSV in the shared volume."""
    if not data:
        print(f"No data collected for {year}")
        return None
    
    df = pd.DataFrame(data)
    df["timeObserved"] = pd.to_datetime(df["timeObserved"], format="ISO8601")
    df = df.sort_values(["stationId", "timeObserved"])
    
    # Remove any rows where mean_radiation is null (extra safety)
    original_count = len(df)
    df = df.dropna(subset=['mean_radiation'])
    cleaned_count = len(df)
    
    if original_count != cleaned_count:
        print(f"Cleaned {original_count - cleaned_count} null radiation records")
    
    # Escribe el archivo en la ruta del volumen compartido
    filename = f"{SHARED_DATA_PATH}/{year}_dmi_sun.csv"
    df.to_csv(filename, index=False)
    
    print(f"\nSaved {len(df)} sun records to shared volume path: {filename}")
    print(f"Date range: {df['timeObserved'].min()} to {df['timeObserved'].max()}")
    print(f"Stations: {df['stationId'].nunique()}")
    print(f"Average mean_radiation: {df['mean_radiation'].mean():.2f}")
    print(f"Max mean_radiation: {df['mean_radiation'].max():.2f}")
    
    return filename

def main():
    """Main function."""
    print("DMI Sun Collection")
    print(f"Target Years: {', '.join(map(str, TARGET_YEARS))}")

    setup_directories()

    for TARGET_YEAR in TARGET_YEARS:
        print("\n" + "="*80)
        print(f"Processing year {TARGET_YEAR}")

        # Usar la nueva ruta de archivo para verificar la existencia
        filename = f"{SHARED_DATA_PATH}/{TARGET_YEAR}_dmi_sun.csv"
        if os.path.exists(filename):
            print(f"File {filename} already exists in shared volume!")
            os.remove(filename)
            print("Removed existing file.")

        year_start = datetime.now()

        # Collect mean radiation data for this year
        try:
            radiation_data = collect_radiation_data(TARGET_YEAR)
        except Exception as e:
            print(f"Error during collection for {TARGET_YEAR}: {e}")
            continue

        # Save to CSV
        output_file = save_data(radiation_data, TARGET_YEAR)

        year_end = datetime.now()
        print(f"Time for {TARGET_YEAR}: {year_end - year_start}")

    print("\nAll done.")
    print("Clean data ready for Hive processing!")

if __name__ == "__main__":
    main()