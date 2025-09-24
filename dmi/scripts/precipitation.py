from collections import defaultdict
import os

def get_precipitation_stations_csv(api_key, output_file="dmi_precipitation_stations.csv"):
    """
    Fetches all DMI stations that measure precipitation (acc_precip) and exports to CSV

    Args:
        api_key (str): Your DMI API key
        output_file (str): Output CSV filename
    """

    print("🌧️ Fetching precipitation measurement data from DMI...")

    base_url = "https://dmigw.govcloud.dk/v2/climateData/collections/stationValue/items"
    station_info = defaultdict(lambda: {
        'stationId': None,
        'name': None,
        'latitude': None,
        'longitude': None,
        'country': None,
        'status': None,
        'validFrom': None,
        'validTo': None,
        'measures_precipitation': True,
        'precipitation_observations': 0,
        'first_precipitation_date': None,
        'last_precipitation_date': None,
        'min_precipitation': None,
        'max_precipitation': None,
        'total_precipitation_sample': 0
    })

    # Get precipitation data with pagination to capture all stations
    print("💧 Getting precipitation stations (acc_precip parameter)...")

    total_observations = 0
    offset = 0
    max_offset = 1000000  # Safety limit
    stations_found = set()

    while offset < max_offset:
        precipitation_params = {
            'api-key': api_key,
            'parameterId': 'acc_precip',
            'limit': 300000,
            'offset': offset
        }

        try:
            print(f"   📊 Fetching data with offset {offset:,}...")
            precipitation_response = requests.get(base_url, params=precipitation_params)
            precipitation_response.raise_for_status()
            precipitation_data = precipitation_response.json()

            features = precipitation_data.get('features', [])
            if not features:
                print(f"   ✅ No more data at offset {offset:,}")
                break

            batch_stations = set()
            for feature in features:
                try:
                    props = feature.get('properties', {})
                    geometry = feature.get('geometry')

                    # Skip if no properties or station ID
                    if not props or 'stationId' not in props:
                        continue

                    station_id = props['stationId']

                    # Track this station
                    batch_stations.add(station_id)
                    stations_found.add(station_id)

                    # Get coordinates if available
                    coords = None
                    if geometry and geometry.get('coordinates'):
                        coords = geometry['coordinates']

                    # Initialize or update station info
                    if station_info[station_id]['stationId'] is None:
                        station_info[station_id].update({
                            'stationId': station_id,
                            'latitude': coords[1] if coords else None,
                            'longitude': coords[0] if coords else None
                        })

                    # If we don't have coordinates yet but this feature does, update them
                    elif coords and station_info[station_id]['latitude'] is None:
                        station_info[station_id]['latitude'] = coords[1]
                        station_info[station_id]['longitude'] = coords[0]

                except (KeyError, TypeError, IndexError) as e:
                    # Skip malformed features but continue processing
                    print(f"     ⚠️  Skipping malformed feature: {e}")
                    continue
                # Track observation details
                from_date = props.get('from')
                precipitation_value = props.get('value')

                if from_date:
                    if not station_info[station_id]['first_precipitation_date'] or from_date < station_info[station_id][
                        'first_precipitation_date']:
                        station_info[station_id]['first_precipitation_date'] = from_date
                    if not station_info[station_id]['last_precipitation_date'] or from_date > station_info[station_id][
                        'last_precipitation_date']:
                        station_info[station_id]['last_precipitation_date'] = from_date

                # Track precipitation values for statistics
                if precipitation_value is not None:
                    station_info[station_id]['total_precipitation_sample'] += precipitation_value

                    if station_info[station_id]['min_precipitation'] is None or precipitation_value < \
                            station_info[station_id]['min_precipitation']:
                        station_info[station_id]['min_precipitation'] = precipitation_value

                    if station_info[station_id]['max_precipitation'] is None or precipitation_value > \
                            station_info[station_id]['max_precipitation']:
                        station_info[station_id]['max_precipitation'] = precipitation_value

                station_info[station_id]['precipitation_observations'] += 1
                total_observations += 1

            print(f"   📈 Found {len(features):,} observations from {len(batch_stations)} stations")
            print(f"   🏢 Total unique stations so far: {len(stations_found)}")

            # Move to next batch
            offset += 300000

            # If we got less than the limit, we're probably done
            if len(features) < 300000:
                print(f"   ✅ Retrieved {len(features):,} observations (less than limit), probably done")
                break

        except requests.exceptions.RequestException as e:
            print(f"   ❌ Error fetching precipitation data at offset {offset:,}: {e}")
            if "400" in str(e):
                print(f"   ℹ️  Offset {offset:,} not supported, stopping pagination")
                break
            else:
                break

    print(f"\n📊 Precipitation data collection complete:")
    print(f"   • Total observations processed: {total_observations:,}")
    print(f"   • Unique stations found: {len(stations_found)}")

    # Get station details
    print("\n📍 Getting station details...")
    station_url = "https://dmigw.govcloud.dk/v2/climateData/collections/station/items"
    station_params = {
        'api-key': api_key,
        'limit': 300000
    }

    try:
        station_response = requests.get(station_url, params=station_params)
        station_response.raise_for_status()
        all_stations_data = station_response.json()

        # Match station details with precipitation stations
        matched_stations = 0
        for feature in all_stations_data.get('features', []):
            props = feature['properties']
            station_id = props['stationId']

            if station_id in station_info:
                station_info[station_id]['name'] = props.get('name', 'Unknown')
                station_info[station_id]['country'] = props.get('country', 'Unknown')
                station_info[station_id]['status'] = props.get('status', 'Unknown')
                station_info[station_id]['validFrom'] = props.get('validFrom')
                station_info[station_id]['validTo'] = props.get('validTo')
                matched_stations += 1

        print(f"   📋 Retrieved details for {matched_stations}/{len(stations_found)} precipitation stations")

    except requests.exceptions.RequestException as e:
        print(f"   ⚠️  Warning: Could not fetch station details: {e}")

    # Create DataFrame and export to CSV
    print("\n💾 Creating CSV file...")

    # Convert to list of dictionaries for DataFrame
    stations_list = []
    for station_id, info in station_info.items():
        # Calculate average precipitation (rough estimate)
        avg_precipitation = None
        if info['precipitation_observations'] > 0 and info['total_precipitation_sample'] > 0:
            avg_precipitation = info['total_precipitation_sample'] / info['precipitation_observations']

        stations_list.append({
            'station_id': info['stationId'],
            'name': info.get('name', 'Unknown'),
            'country': info.get('country', 'Unknown'),
            'latitude': info['latitude'],
            'longitude': info['longitude'],
            'status': info.get('status', 'Unknown'),
            'valid_from': info.get('validFrom'),
            'valid_to': info.get('validTo'),
            'precipitation_observations': info['precipitation_observations'],
            'first_precipitation_date': info['first_precipitation_date'],
            'last_precipitation_date': info['last_precipitation_date'],
            'min_precipitation_mm': info['min_precipitation'],
            'max_precipitation_mm': info['max_precipitation'],
            'avg_precipitation_mm': round(avg_precipitation, 2) if avg_precipitation else None,
            'total_precipitation_sample_mm': round(info['total_precipitation_sample'], 2) if info[
                'total_precipitation_sample'] else None
        })

    # Create DataFrame
    df = pd.DataFrame(stations_list)

    # Sort by country, then by station_id
    df = df.sort_values(['country', 'station_id'])

    # Export to CSV
    df.to_csv(output_file, index=False)

    # Print detailed summary
    print(f"\n✅ Successfully exported {len(df)} precipitation stations to '{output_file}'")
    print(f"\n📈 Detailed Summary:")
    print(f"   • Total precipitation stations: {len(df)}")
    print(f"   • Total observations processed: {total_observations:,}")

    # Status breakdown
    status_counts = df['status'].value_counts()
    print(f"   • Station status breakdown:")
    for status, count in status_counts.items():
        print(f"     - {status}: {count}")

    # Country breakdown
    country_counts = df['country'].value_counts()
    print(f"   • Stations by country:")
    for country, count in country_counts.items():
        print(f"     - {country}: {count}")

    # Data range
    all_first_dates = df['first_precipitation_date'].dropna()
    all_last_dates = df['last_precipitation_date'].dropna()

    if len(all_first_dates) > 0 and len(all_last_dates) > 0:
        earliest_date = min(all_first_dates)[:10]  # Just date part
        latest_date = max(all_last_dates)[:10]
        print(f"   • Data time range: {earliest_date} to {latest_date}")

    # Top stations by observation count
    top_stations = df.nlargest(5, 'precipitation_observations')[
        ['station_id', 'name', 'country', 'precipitation_observations']]
    print(f"   • Top 5 stations by observation count:")
    for _, station in top_stations.iterrows():
        print(
            f"     - {station['name']} ({station['station_id']}, {station['country']}): {station['precipitation_observations']:,} observations")

    return True


def main():
    """
    Main function to run the script
    """
    print("🌧️ DMI Precipitation Stations CSV Export Tool")
    print("=" * 55)
    print("Parameter: acc_precip (Accumulated precipitation)")
    print()

    # Get API key from user
    api_key = (os.environ.get('DMI_API_KEY') or input("Enter your DMI API key: ")).strip()

    if not api_key:
        print("❌ API key is required!")
        return

    # Optional: custom output filename
    output_file = input("Enter output filename (press Enter for 'dmi_precipitation_stations.csv'): ").strip()
    if not output_file:
        output_file = "../datasets/dmi_precipitation_stations.csv"

    # Run the export
    success = get_precipitation_stations_csv(api_key, output_file)

    if success:
        print(f"\n🎉 Done! Check '{output_file}' for your precipitation stations data.")
        print("\nCSV columns include:")
        print("• station_id, name, country, latitude, longitude")
        print("• status, valid_from, valid_to")
        print("• precipitation_observations, first/last_precipitation_date")
        print("• min/max/avg precipitation values (mm)")
        print("• total_precipitation_sample_mm")
    else:
        print("❌ Export failed. Please check your API key and try again.")


if __name__ == "__main__":
    # Required packages
    try:
        import pandas as pd
        import requests
    except ImportError as e:
        print("❌ Missing required packages. Install with:")
        print("pip install pandas requests")
        exit(1)

    main()