from collections import defaultdict
import os


def get_sunlight_stations_csv(api_key, output_file="dmi_sunlight_stations.csv"):
    """
    Fetches all DMI stations that measure sunlight parameters and exports to CSV

    Args:
        api_key (str): Your DMI API key
        output_file (str): Output CSV filename
    """

    print("🔍 Fetching sunlight measurement data from DMI...")

    base_url = "https://dmigw.govcloud.dk/v2/climateData/collections/stationValue/items"
    station_info = defaultdict(lambda: {
        'stationId': None,
        'name': None,
        'latitude': None,
        'longitude': None,
        'measures_sunshine': False,
        'measures_radiation': False,
        'sunshine_observations': 0,
        'radiation_observations': 0,
        'first_sunshine_date': None,
        'last_sunshine_date': None,
        'first_radiation_date': None,
        'last_radiation_date': None
    })

    # Step 1: Get bright sunshine data
    print("📊 Getting bright sunshine stations...")
    sunshine_params = {
        'api-key': api_key,
        'parameterId': 'bright_sunshine',
        'limit': 300000
    }

    try:
        sunshine_response = requests.get(base_url, params=sunshine_params)
        sunshine_response.raise_for_status()
        sunshine_data = sunshine_response.json()

        sunshine_count = 0
        for feature in sunshine_data.get('features', []):
            props = feature['properties']
            coords = feature['geometry']['coordinates']
            station_id = props['stationId']

            station_info[station_id].update({
                'stationId': station_id,
                'latitude': coords[1],
                'longitude': coords[0],
                'measures_sunshine': True
            })

            # Track observation dates
            from_date = props.get('from')
            if from_date:
                if not station_info[station_id]['first_sunshine_date'] or from_date < station_info[station_id][
                    'first_sunshine_date']:
                    station_info[station_id]['first_sunshine_date'] = from_date
                if not station_info[station_id]['last_sunshine_date'] or from_date > station_info[station_id][
                    'last_sunshine_date']:
                    station_info[station_id]['last_sunshine_date'] = from_date

            station_info[station_id]['sunshine_observations'] += 1
            sunshine_count += 1

        print(
            f"   ☀️ Found {len(set(f['properties']['stationId'] for f in sunshine_data.get('features', [])))} stations with {sunshine_count:,} sunshine observations")

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching sunshine data: {e}")
        return False

    # Step 2: Get solar radiation data
    print("🌞 Getting solar radiation stations...")
    radiation_params = {
        'api-key': api_key,
        'parameterId': 'mean_radiation',
        'limit': 300000
    }

    try:
        radiation_response = requests.get(base_url, params=radiation_params)
        radiation_response.raise_for_status()
        radiation_data = radiation_response.json()

        radiation_count = 0
        for feature in radiation_data.get('features', []):
            props = feature['properties']
            coords = feature['geometry']['coordinates']
            station_id = props['stationId']

            if station_id not in station_info:
                station_info[station_id].update({
                    'stationId': station_id,
                    'latitude': coords[1],
                    'longitude': coords[0]
                })

            station_info[station_id]['measures_radiation'] = True

            # Track observation dates
            from_date = props.get('from')
            if from_date:
                if not station_info[station_id]['first_radiation_date'] or from_date < station_info[station_id][
                    'first_radiation_date']:
                    station_info[station_id]['first_radiation_date'] = from_date
                if not station_info[station_id]['last_radiation_date'] or from_date > station_info[station_id][
                    'last_radiation_date']:
                    station_info[station_id]['last_radiation_date'] = from_date

            station_info[station_id]['radiation_observations'] += 1
            radiation_count += 1

        print(
            f"   🔆 Found {len(set(f['properties']['stationId'] for f in radiation_data.get('features', [])))} stations with {radiation_count:,} radiation observations")

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching radiation data: {e}")
        return False

    # Step 3: Get station details
    print("📍 Getting station details...")
    station_url = "https://dmigw.govcloud.dk/v2/climateData/collections/station/items"
    station_params = {
        'api-key': api_key,
        'limit': 300000
    }

    try:
        station_response = requests.get(station_url, params=station_params)
        station_response.raise_for_status()
        all_stations_data = station_response.json()

        # Match station details with sunlight stations
        for feature in all_stations_data.get('features', []):
            props = feature['properties']
            station_id = props['stationId']

            if station_id in station_info:
                station_info[station_id]['name'] = props.get('name', 'Unknown')
                station_info[station_id]['country'] = props.get('country', 'Unknown')
                station_info[station_id]['status'] = props.get('status', 'Unknown')
                station_info[station_id]['validFrom'] = props.get('validFrom')
                station_info[station_id]['validTo'] = props.get('validTo')

        print(f"   📋 Retrieved details for {len(all_stations_data.get('features', []))} stations")

    except requests.exceptions.RequestException as e:
        print(f"⚠️  Warning: Could not fetch station details: {e}")

    # Step 4: Create DataFrame and export to CSV
    print("💾 Creating CSV file...")

    # Convert to list of dictionaries for DataFrame
    stations_list = []
    for station_id, info in station_info.items():
        stations_list.append({
            'station_id': info['stationId'],
            'name': info.get('name', 'Unknown'),
            'country': info.get('country', 'Unknown'),
            'latitude': info['latitude'],
            'longitude': info['longitude'],
            'status': info.get('status', 'Unknown'),
            'valid_from': info.get('validFrom'),
            'valid_to': info.get('validTo'),
            'measures_sunshine': info['measures_sunshine'],
            'measures_radiation': info['measures_radiation'],
            'sunshine_observations': info['sunshine_observations'],
            'radiation_observations': info['radiation_observations'],
            'first_sunshine_date': info['first_sunshine_date'],
            'last_sunshine_date': info['last_sunshine_date'],
            'first_radiation_date': info['first_radiation_date'],
            'last_radiation_date': info['last_radiation_date'],
            'measurement_types': ', '.join([
                'bright_sunshine' if info['measures_sunshine'] else '',
                'mean_radiation' if info['measures_radiation'] else ''
            ]).strip(', ')
        })

    # Create DataFrame
    df = pd.DataFrame(stations_list)

    # Sort by station_id
    df = df.sort_values('station_id')

    # Export to CSV
    df.to_csv(output_file, index=False)

    # Print summary
    print(f"\n✅ Successfully exported {len(df)} sunlight stations to '{output_file}'")
    print(f"\n📈 Summary:")
    print(f"   • Total sunlight stations: {len(df)}")
    print(f"   • Sunshine only: {len(df[df['measures_sunshine'] & ~df['measures_radiation']])}")
    print(f"   • Radiation only: {len(df[~df['measures_sunshine'] & df['measures_radiation']])}")
    print(f"   • Both measurements: {len(df[df['measures_sunshine'] & df['measures_radiation']])}")
    print(f"   • Active stations: {len(df[df['status'] == 'Active'])}")
    print(f"   • Countries: {', '.join(df['country'].unique())}")

    return True


def main():
    print("🇩🇰 DMI Sunlight Stations CSV Export Tool")
    print("=" * 50)

    # Get API key from user
    api_key = (os.environ.get('DMI_API_KEY') or input("Enter your DMI API key: ")).strip()

    if not api_key:
        print("❌ API key is required!")
        return

    # Optional: custom output filename
    output_file = input("Enter output filename (press Enter for 'dmi_sunlight_stations.csv'): ").strip()
    if not output_file:
        output_file = "../datasets/dmi_sunlight_stations.csv"

    # Run the export
    success = get_sunlight_stations_csv(api_key, output_file)

    if success:
        print(f"\n🎉 Done! Check '{output_file}' for your sunlight stations data.")
        print("\nCSV columns include:")
        print("• station_id, name, country, latitude, longitude")
        print("• status, valid_from, valid_to")
        print("• measures_sunshine, measures_radiation")
        print("• sunshine_observations, radiation_observations")
        print("• first/last observation dates for each parameter")
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