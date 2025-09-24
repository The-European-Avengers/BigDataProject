import os
from collections import defaultdict


def get_wind_stations_csv(api_key, output_file="dmi_wind_stations.csv"):
    """
    Fetches all DMI stations that measure wind parameters and exports to CSV

    Wind Parameters:
    - mean_wind_speed: Mean wind speed
    - max_wind_speed_3sec: Maximum wind speed (3 seconds average)
    - max_wind_speed_10min: Maximum wind speed (10 minutes average)
    - mean_wind_dir: Mean wind direction
    - mean_wind_dir_min0: Mean wind direction (10 minutes average) at minute 0

    Args:
        api_key (str): Your DMI API key
        output_file (str): Output CSV filename
    """

    print("💨 Fetching wind measurement data from DMI...")

    # Define all wind parameters
    wind_parameters = {
        'mean_wind_speed': 'Mean wind speed (m/s)',
        'max_wind_speed_3sec': 'Maximum wind speed - 3 second average (m/s)',
        'max_wind_speed_10min': 'Maximum wind speed - 10 minute average (m/s)',
        'mean_wind_dir': 'Mean wind direction (degrees)',
        'mean_wind_dir_min0': 'Mean wind direction at minute 0 (degrees)'
    }

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
        'wind_parameters': set(),
        'total_wind_observations': 0,
        'mean_wind_speed_obs': 0,
        'max_wind_speed_3sec_obs': 0,
        'max_wind_speed_10min_obs': 0,
        'mean_wind_dir_obs': 0,
        'mean_wind_dir_min0_obs': 0,
        'first_wind_date': None,
        'last_wind_date': None,
        'max_wind_speed_recorded': None,
        'min_wind_speed_recorded': None
    })

    total_observations = 0
    all_stations_found = set()

    # Process each wind parameter
    for param_id, param_description in wind_parameters.items():
        print(f"\n🌬️ Processing {param_id} ({param_description})...")

        offset = 0
        max_offset = 1000000
        param_observations = 0
        param_stations = set()

        while offset < max_offset:
            wind_params = {
                'api-key': api_key,
                'parameterId': param_id,
                'limit': 300000,
                'offset': offset
            }

            try:
                print(f"   📊 Fetching {param_id} data with offset {offset:,}...")
                response = requests.get(base_url, params=wind_params)
                response.raise_for_status()
                data = response.json()

                features = data.get('features', [])
                if not features:
                    print(f"   ✅ No more {param_id} data at offset {offset:,}")
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
                        param_stations.add(station_id)
                        all_stations_found.add(station_id)

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

                        # Add this parameter to the station's capabilities
                        station_info[station_id]['wind_parameters'].add(param_id)

                        # Track observation details
                        from_date = props.get('from')
                        wind_value = props.get('value')

                        if from_date:
                            if not station_info[station_id]['first_wind_date'] or from_date < station_info[station_id][
                                'first_wind_date']:
                                station_info[station_id]['first_wind_date'] = from_date
                            if not station_info[station_id]['last_wind_date'] or from_date > station_info[station_id][
                                'last_wind_date']:
                                station_info[station_id]['last_wind_date'] = from_date

                        # Track wind speed values for statistics (only for speed parameters)
                        if wind_value is not None and 'speed' in param_id:
                            if station_info[station_id]['max_wind_speed_recorded'] is None or wind_value > \
                                    station_info[station_id]['max_wind_speed_recorded']:
                                station_info[station_id]['max_wind_speed_recorded'] = wind_value

                            if station_info[station_id]['min_wind_speed_recorded'] is None or wind_value < \
                                    station_info[station_id]['min_wind_speed_recorded']:
                                station_info[station_id]['min_wind_speed_recorded'] = wind_value

                        # Update parameter-specific observation counts
                        station_info[station_id][f'{param_id}_obs'] += 1
                        station_info[station_id]['total_wind_observations'] += 1
                        param_observations += 1
                        total_observations += 1

                    except (KeyError, TypeError, IndexError) as e:
                        # Skip malformed features but continue processing
                        continue

                print(f"   📈 Found {len(features):,} observations from {len(batch_stations)} stations")

                # Move to next batch
                offset += 300000

                # If we got less than the limit, we're probably done
                if len(features) < 300000:
                    print(f"   ✅ Retrieved {len(features):,} observations (less than limit), done with {param_id}")
                    break

            except requests.exceptions.RequestException as e:
                print(f"   ❌ Error fetching {param_id} data at offset {offset:,}: {e}")
                if "400" in str(e):
                    print(f"   ℹ️  Offset {offset:,} not supported for {param_id}, stopping pagination")
                    break
                else:
                    break

        print(f"   📊 {param_id} summary: {param_observations:,} observations from {len(param_stations)} stations")

    print(f"\n📊 All wind parameters processed:")
    print(f"   • Total observations: {total_observations:,}")
    print(f"   • Total unique wind stations: {len(all_stations_found)}")

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

        # Match station details with wind stations
        matched_stations = 0
        for feature in all_stations_data.get('features', []):
            props = feature.get('properties', {})
            if not props:
                continue

            station_id = props.get('stationId')
            if station_id in station_info:
                station_info[station_id]['name'] = props.get('name', 'Unknown')
                station_info[station_id]['country'] = props.get('country', 'Unknown')
                station_info[station_id]['status'] = props.get('status', 'Unknown')
                station_info[station_id]['validFrom'] = props.get('validFrom')
                station_info[station_id]['validTo'] = props.get('validTo')
                matched_stations += 1

        print(f"   📋 Retrieved details for {matched_stations}/{len(all_stations_found)} wind stations")

    except requests.exceptions.RequestException as e:
        print(f"   ⚠️  Warning: Could not fetch station details: {e}")

    # Create DataFrame and export to CSV
    print("\n💾 Creating CSV file...")

    # Convert to list of dictionaries for DataFrame
    stations_list = []
    for station_id, info in station_info.items():
        # Create parameter flags
        wind_params_str = ', '.join(sorted(info['wind_parameters']))

        stations_list.append({
            'station_id': info['stationId'],
            'name': info.get('name', 'Unknown'),
            'country': info.get('country', 'Unknown'),
            'latitude': info['latitude'],
            'longitude': info['longitude'],
            'status': info.get('status', 'Unknown'),
            'valid_from': info.get('validFrom'),
            'valid_to': info.get('validTo'),
            'wind_parameters_measured': wind_params_str,
            'parameter_count': len(info['wind_parameters']),
            'measures_mean_wind_speed': 'mean_wind_speed' in info['wind_parameters'],
            'measures_max_wind_speed_3sec': 'max_wind_speed_3sec' in info['wind_parameters'],
            'measures_max_wind_speed_10min': 'max_wind_speed_10min' in info['wind_parameters'],
            'measures_mean_wind_dir': 'mean_wind_dir' in info['wind_parameters'],
            'measures_mean_wind_dir_min0': 'mean_wind_dir_min0' in info['wind_parameters'],
            'total_wind_observations': info['total_wind_observations'],
            'mean_wind_speed_observations': info['mean_wind_speed_obs'],
            'max_wind_speed_3sec_observations': info['max_wind_speed_3sec_obs'],
            'max_wind_speed_10min_observations': info['max_wind_speed_10min_obs'],
            'mean_wind_dir_observations': info['mean_wind_dir_obs'],
            'mean_wind_dir_min0_observations': info['mean_wind_dir_min0_obs'],
            'first_wind_date': info['first_wind_date'],
            'last_wind_date': info['last_wind_date'],
            'max_wind_speed_recorded_ms': info['max_wind_speed_recorded'],
            'min_wind_speed_recorded_ms': info['min_wind_speed_recorded']
        })

    # Create DataFrame
    df = pd.DataFrame(stations_list)

    # Sort by country, then by parameter count (most comprehensive stations first), then by station_id
    df = df.sort_values(['country', 'parameter_count', 'station_id'], ascending=[True, False, True])

    # Export to CSV
    df.to_csv(output_file, index=False)

    # Print detailed summary
    print(f"\n✅ Successfully exported {len(df)} wind stations to '{output_file}'")
    print(f"\n📈 Detailed Summary:")
    print(f"   • Total wind stations: {len(df)}")
    print(f"   • Total wind observations processed: {total_observations:,}")

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

    # Parameter coverage analysis
    print(f"   • Wind parameter coverage:")
    for param_id, param_desc in wind_parameters.items():
        param_column = f'measures_{param_id}'
        if param_column in df.columns:
            count = df[param_column].sum()
            print(f"     - {param_id}: {count} stations")

    # Comprehensive stations (measure all parameters)
    all_params_count = df[df['parameter_count'] == len(wind_parameters)]
    print(f"   • Stations measuring ALL wind parameters: {len(all_params_count)}")

    # Data range
    all_first_dates = df['first_wind_date'].dropna()
    all_last_dates = df['last_wind_date'].dropna()

    if len(all_first_dates) > 0 and len(all_last_dates) > 0:
        earliest_date = min(all_first_dates)[:10]  # Just date part
        latest_date = max(all_last_dates)[:10]
        print(f"   • Wind data time range: {earliest_date} to {latest_date}")

    # Top stations by observation count
    top_stations = df.nlargest(5, 'total_wind_observations')[
        ['station_id', 'name', 'country', 'parameter_count', 'total_wind_observations']]
    print(f"   • Top 5 stations by total wind observations:")
    for _, station in top_stations.iterrows():
        print(
            f"     - {station['name']} ({station['station_id']}, {station['country']}): {station['total_wind_observations']:,} obs, {station['parameter_count']} parameters")

    # Wind speed extremes
    if df['max_wind_speed_recorded_ms'].notna().any():
        max_speed_station = df.loc[df['max_wind_speed_recorded_ms'].idxmax()]
        min_speed_station = df.loc[df['min_wind_speed_recorded_ms'].idxmin()]
        print(
            f"   • Highest wind speed recorded: {max_speed_station['max_wind_speed_recorded_ms']:.1f} m/s at {max_speed_station['name']}")
        print(
            f"   • Lowest wind speed recorded: {min_speed_station['min_wind_speed_recorded_ms']:.1f} m/s at {min_speed_station['name']}")

    return True


def main():
    """
    Main function to run the script
    """
    print("💨 DMI Wind Stations CSV Export Tool")
    print("=" * 50)
    print("Wind Parameters:")
    print("• mean_wind_speed - Mean wind speed")
    print("• max_wind_speed_3sec - Max wind speed (3 sec avg)")
    print("• max_wind_speed_10min - Max wind speed (10 min avg)")
    print("• mean_wind_dir - Mean wind direction")
    print("• mean_wind_dir_min0 - Mean wind direction at minute 0")
    print()

    # Get API key from user
    api_key = (os.environ.get('DMI_API_KEY') or input("Enter your DMI API key: ")).strip()

    if not api_key:
        print("❌ API key is required!")
        return

    # Optional: custom output filename
    output_file = input("Enter output filename (press Enter for 'dmi_wind_stations.csv'): ").strip()
    if not output_file:
        output_file = "../datasets/dmi_wind_stations.csv"

    print(f"\n🚀 Starting wind station discovery...")
    print("This may take several minutes as we process 5 wind parameters...")

    # Run the export
    success = get_wind_stations_csv(api_key, output_file)

    if success:
        print(f"\n🎉 Done! Check '{output_file}' for your wind stations data.")
        print("\nCSV columns include:")
        print("• Basic info: station_id, name, country, coordinates, status")
        print("• Parameter flags: measures_[parameter] (TRUE/FALSE for each wind param)")
        print("• Observation counts: per parameter and total")
        print("• Date ranges: first/last wind observation dates")
        print("• Wind statistics: max/min wind speeds recorded")
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