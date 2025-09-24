"""
Robust Weather Stations Mapper - Simplified & Reliable
======================================================

This version focuses on reliability and ensures all markers actually appear on the map.
Uses simplified approach to avoid JavaScript/HTML rendering issues.
"""

import pandas as pd
import folium
import os
from typing import Tuple
import warnings
warnings.filterwarnings('ignore')


class RobustWeatherMapper:
    """
    Simplified, reliable weather stations mapper that ensures markers actually appear.
    """

    def __init__(self, map_center: Tuple[float, float] = None, zoom_start: int = 6):
        """Initialize the mapper."""
        self.station_data = {
            'wind': pd.DataFrame(),
            'precipitation': pd.DataFrame(),
            'sunlight': pd.DataFrame()
        }

        self.colors = {
            'wind': 'red',
            'precipitation': 'blue',
            'sunlight': 'orange'
        }

        self.icons = {
            'wind': 'leaf',
            'precipitation': 'tint',
            'sunlight': 'sun'
        }

        self.map_center = map_center
        self.zoom_start = zoom_start

    def load_csv(self, file_path: str, station_type: str) -> None:
        """Load CSV data with automatic column detection."""
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return

        try:
            df = pd.read_csv(file_path)
            print(f"📂 Loading {len(df)} {station_type} stations from {file_path}")

            # Find coordinate columns
            lat_col = None
            lng_col = None
            name_col = None

            for col in df.columns:
                if 'lat' in col.lower() and lat_col is None:
                    lat_col = col
                elif ('lon' in col.lower() or 'lng' in col.lower()) and lng_col is None:
                    lng_col = col
                elif 'name' in col.lower() and name_col is None:
                    name_col = col

            if not lat_col or not lng_col:
                print(f"❌ Could not find coordinate columns in {file_path}")
                return

            # Clean and prepare data
            df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce')
            df[lng_col] = pd.to_numeric(df[lng_col], errors='coerce')

            # Remove invalid coordinates
            df = df.dropna(subset=[lat_col, lng_col])

            # Standardize column names
            df = df.rename(columns={
                lat_col: 'latitude',
                lng_col: 'longitude',
                name_col: 'name' if name_col else 'name'
            })

            if 'name' not in df.columns:
                df['name'] = f"{station_type.title()} Station " + df.index.astype(str)

            # Add station type
            df['station_type'] = station_type

            self.station_data[station_type] = df
            print(f"   ✅ Successfully loaded {len(df)} {station_type} stations")

        except Exception as e:
            print(f"❌ Error loading {file_path}: {e}")



    def create_combined_map(self, save_path: str = 'combined_weather_map.html') -> folium.Map:
        """Create map with combined stations at same locations."""
        print("\n🗺️  Creating combined stations map...")

        # Combine all data first
        all_stations = []
        for station_type, df in self.station_data.items():
            if len(df) > 0:
                all_stations.append(df)

        if not all_stations:
            print("❌ No station data!")
            return None

        combined_df = pd.concat(all_stations, ignore_index=True)

        # Group by coordinates (rounded to avoid floating point issues)
        combined_df['lat_rounded'] = combined_df['latitude'].round(6)
        combined_df['lng_rounded'] = combined_df['longitude'].round(6)
        combined_df['coord_key'] = combined_df['lat_rounded'].astype(str) + ',' + combined_df['lng_rounded'].astype(str)

        # Process groups
        location_groups = combined_df.groupby('coord_key')

        print(f"   🔍 Found {len(location_groups)} unique locations")

        # Calculate map center
        if self.map_center is None:
            self.map_center = [combined_df['latitude'].mean(), combined_df['longitude'].mean()]

        # Create map
        m = folium.Map(
            location=self.map_center,
            zoom_start=self.zoom_start,
            tiles='OpenStreetMap'
        )

        single_stations = 0
        multi_stations = 0

        for coord_key, group in location_groups:
            try:
                # Get location info
                first_station = group.iloc[0]
                lat = float(first_station['latitude'])
                lng = float(first_station['longitude'])

                station_types = group['station_type'].unique().tolist()
                station_names = group['name'].unique().tolist()

                # Determine marker style
                if len(station_types) == 1:
                    # Single type
                    color = self.colors[station_types[0]]
                    icon = self.icons[station_types[0]]
                    single_stations += 1
                else:
                    # Multiple types
                    color = 'purple'
                    icon = 'star'
                    multi_stations += 1

                # Create popup
                popup_html = f"""
                <div style="font-family: Arial; min-width: 250px;">
                    <h4 style="color: {color}; margin-bottom: 10px;">
                        {station_names[0]}
                    </h4>
                    <p><strong>Location:</strong> {lat:.4f}, {lng:.4f}</p>
                    <p><strong>Measurements:</strong> {', '.join(station_types)}</p>
                    <p><strong>Station Types:</strong> {len(station_types)}</p>
                """

                # Add details for each type
                for station_type in station_types:
                    type_data = group[group['station_type'] == station_type].iloc[0]
                    popup_html += f"<hr><p><strong>{station_type.title()}:</strong></p>"
                    if 'status' in type_data:
                        popup_html += f"<p>Status: {type_data['status']}</p>"

                popup_html += "</div>"

                # Create marker
                marker = folium.Marker(
                    location=[lat, lng],
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"{station_names[0]} ({', '.join(station_types)})",
                    icon=folium.Icon(
                        color=color,
                        icon=icon,
                        prefix='fa'
                    )
                )

                marker.add_to(m)

            except Exception as e:
                print(f"   ⚠️ Error processing location {coord_key}: {e}")

        print(f"   ✅ Added {single_stations} single-type + {multi_stations} multi-type locations")

        # Add legend
        legend_html = f'''
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: 120px; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 10px">
        <p><b>Weather Stations Map</b></p>
        <p><i class="fa fa-leaf" style="color:red"></i> Wind Only</p>
        <p><i class="fa fa-tint" style="color:blue"></i> Precipitation Only</p>
        <p><i class="fa fa-sun" style="color:orange"></i> Sunlight Only</p>
        <p><i class="fa fa-star" style="color:purple"></i> Multiple Types</p>
        <hr>
        <p><b>Total Locations:</b> {single_stations + multi_stations}</p>
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))

        # Save map
        m.save(save_path)
        print(f"\n🎉 Combined map saved: {save_path}")
        print(f"📊 {single_stations} single-type + {multi_stations} multi-type locations")

        return m


def main():
    """Create both simple and combined weather station maps."""
    print("🌦️  Robust Weather Stations Mapper")
    print("=" * 45)

    mapper = RobustWeatherMapper()

    # Load CSV files
    csv_files = {
        'wind': 'datasets/dmi_wind_stations.csv',
        'precipitation': 'datasets/dmi_precipitation_stations.csv',
        'sunlight': 'datasets/dmi_sunlight_stations.csv'
    }

    loaded_any = False
    for station_type, file_path in csv_files.items():
        if os.path.exists(file_path):
            mapper.load_csv(file_path, station_type)
            loaded_any = True
        else:
            print(f"⚠️  File not found: {file_path}")

    if not loaded_any:
        print("❌ No CSV files found!")
        return

    # Create combined map
    print("\n" + "=" * 50)
    combined_map = mapper.create_combined_map('html/robust_combined_weather_map.html')

    print("\n🎊 SUCCESS! Created map:")
    print("   🎯 html/robust_combined_weather_map.html - Combined stations at same locations")


if __name__ == "__main__":
    main()