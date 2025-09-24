import requests
import os

def check_additional_stations(api_key):
    """
    Check if there are more stations by using different API approaches
    """
    print("🔍 Checking for additional sunlight stations...")

    base_url = "https://dmigw.govcloud.dk/v2/climateData/collections"

    # Method 1: Try to get stations with pagination/offset
    print("\n1️⃣ Method 1: Using offset to get more stations...")

    sunshine_stations = set()
    radiation_stations = set()

    # Try multiple offsets for sunshine data
    for offset in [0, 300000, 600000]:
        params = {
            'api-key': api_key,
            'parameterId': 'bright_sunshine',
            'limit': 300000,
            'offset': offset
        }

        try:
            response = requests.get(f"{base_url}/stationValue/items", params=params)
            response.raise_for_status()
            data = response.json()

            station_count_before = len(sunshine_stations)
            for feature in data.get('features', []):
                sunshine_stations.add(feature['properties']['stationId'])

            new_stations = len(sunshine_stations) - station_count_before
            print(
                f"   Offset {offset:,}: Found {len(data.get('features', []))} observations, {new_stations} new stations")

            if len(data.get('features', [])) == 0:
                print(f"   No more data at offset {offset:,}")
                break

        except Exception as e:
            print(f"   Error at offset {offset:,}: {e}")
            break

    # Try multiple offsets for radiation data
    print("\n   Checking radiation stations with offsets...")
    for offset in [0, 300000, 600000]:
        params = {
            'api-key': api_key,
            'parameterId': 'mean_radiation',
            'limit': 300000,
            'offset': offset
        }

        try:
            response = requests.get(f"{base_url}/stationValue/items", params=params)
            response.raise_for_status()
            data = response.json()

            station_count_before = len(radiation_stations)
            for feature in data.get('features', []):
                radiation_stations.add(feature['properties']['stationId'])

            new_stations = len(radiation_stations) - station_count_before
            print(
                f"   Offset {offset:,}: Found {len(data.get('features', []))} observations, {new_stations} new stations")

            if len(data.get('features', [])) == 0:
                print(f"   No more data at offset {offset:,}")
                break

        except Exception as e:
            print(f"   Error at offset {offset:,}: {e}")
            break

    print(f"\n📊 Results from Method 1:")
    print(f"   Unique sunshine stations found: {len(sunshine_stations)}")
    print(f"   Unique radiation stations found: {len(radiation_stations)}")
    print(f"   Total unique sunlight stations: {len(sunshine_stations.union(radiation_stations))}")

    # Method 2: Get ALL stations and check which have sunlight data
    print("\n2️⃣ Method 2: Getting all stations and cross-referencing...")

    try:
        station_params = {
            'api-key': api_key,
            'limit': 300000
        }

        response = requests.get(f"{base_url}/station/items", params=station_params)
        response.raise_for_status()
        all_stations_data = response.json()

        total_stations = len(all_stations_data.get('features', []))
        print(f"   Total DMI stations in database: {total_stations}")

        # Check which countries/regions
        countries = set()
        for feature in all_stations_data.get('features', []):
            country = feature['properties'].get('country', 'Unknown')
            countries.add(country)

        print(f"   Countries represented: {', '.join(sorted(countries))}")

        # Count active vs inactive
        active_count = sum(1 for f in all_stations_data.get('features', [])
                           if f['properties'].get('status') == 'Active')
        print(f"   Active stations: {active_count}/{total_stations}")

    except Exception as e:
        print(f"   Error getting all stations: {e}")

    # Method 3: Try recent time range to see current active stations
    print("\n3️⃣ Method 3: Checking recent data (last 30 days)...")

    from datetime import datetime, timedelta
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    recent_params = {
        'api-key': api_key,
        'parameterId': 'bright_sunshine',
        'datetime': f"{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}",
        'limit': 50000
    }

    try:
        response = requests.get(f"{base_url}/stationValue/items", params=recent_params)
        response.raise_for_status()
        recent_data = response.json()

        recent_sunshine_stations = set()
        for feature in recent_data.get('features', []):
            recent_sunshine_stations.add(feature['properties']['stationId'])

        print(f"   Stations with recent sunshine data: {len(recent_sunshine_stations)}")
        print(f"   Recent observations found: {len(recent_data.get('features', []))}")

        if len(recent_sunshine_stations) > 0:
            print(f"   Sample recent stations: {list(recent_sunshine_stations)[:5]}")

    except Exception as e:
        print(f"   Error checking recent data: {e}")

    # Summary
    print(f"\n🎯 CONCLUSION:")
    total_unique = len(sunshine_stations.union(radiation_stations))
    if total_unique > 35:
        print(f"   ✅ Found MORE stations! Total: {total_unique} (vs your CSV's 35)")
        print(f"   Your original export was limited by the 300,000 observation cap")
    elif total_unique == 35:
        print(f"   ✅ Your CSV contains ALL {total_unique} sunlight stations")
        print(f"   No additional stations were found")
    else:
        print(f"   ⚠️  Found fewer stations ({total_unique}), something might be wrong")

    return sunshine_stations, radiation_stations


def main():
    """
    Main function
    """
    print("🌞 DMI Sunlight Stations - Additional Check")
    print("=" * 50)

    api_key = (os.environ.get('DMI_API_KEY') or input("Enter your DMI API key: ")).strip()

    if not api_key:
        print("❌ API key is required!")
        return

    sunshine_stations, radiation_stations = check_additional_stations(api_key)

    print(f"\n📋 FINAL SUMMARY:")
    print(f"   Stations with sunshine data: {len(sunshine_stations)}")
    print(f"   Stations with radiation data: {len(radiation_stations)}")
    print(f"   Combined unique stations: {len(sunshine_stations.union(radiation_stations))}")

    if len(sunshine_stations.union(radiation_stations)) > 35:
        response = input(
            f"\n🔄 Want to export ALL {len(sunshine_stations.union(radiation_stations))} stations to a new CSV? (y/n): ")
        if response.lower() == 'y':
            print("You can modify the original script to use offsets to get all stations!")


if __name__ == "__main__":
    main()