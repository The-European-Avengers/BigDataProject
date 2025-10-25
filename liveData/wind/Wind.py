# Import libraries
import requests
import pandas as pd
import folium
import geopandas as gpd
from shapely.geometry import Point

# API Parameters
api_key = "9c1728d6-69ec-4be2-8e6c-72dee4dc44df"  
url = "https://dmigw.govcloud.dk/v1/forecastedr/collections/harmonie_dini_sf/cube"

params = {
    "bbox": "7.0,54.5,16.0,58.0",        # All denmark
    "parameter-name": "wind-speed-10m",
    "datetime": "2025-10-17T00:00:00Z/..",
    "crs": "crs84",
    "f": "GeoJSON",
    "api-key": api_key
}

# Call the API
print("Requesting data from API...")
response = requests.get(url, params=params)
if response.status_code != 200:
    raise Exception(f"API request failed: {response.status_code}, {response.text}")

data = response.json()
print("Data retrieved successfully. Number of points:", len(data['features']), "first feature:", data['features'][0])


# Convert GeoJSON a DataFrame
records = []
for feature in data['features']:
    lon, lat = feature['geometry']['coordinates']
    props = feature['properties']
    props.update({"lon": lon, "lat": lat})
    records.append(props)

df = pd.DataFrame(records)


# Show data values as a table
features = data['features']

properties = [
    {
        # Extract coordinates from geometry
        'lat': feature['geometry']['coordinates'][1],
        'lon': feature['geometry']['coordinates'][0],
        # Merge all properties
        **feature['properties']
    }
    for feature in features
]

df = pd.DataFrame(properties)
print("Showing first 5 rows of DataFrame:")
print(df.head().to_string())


print("Starting geospatial filtering...")
# Load shapefile,  shapefile should be in the folder: .shp, .shx, .dbf, .prj)
dk_shape = gpd.read_file("../dk.shp")  
dk_shape = dk_shape.to_crs("EPSG:4326")  

# Convert DataFrame to GeoDataFrame
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="EPSG:4326")


# Filter points, just the ones inside Denmark
gdf = gdf[gdf.within(dk_shape.unary_union)]
print("Number of points after filtering:", len(gdf))

