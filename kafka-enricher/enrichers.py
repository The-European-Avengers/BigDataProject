# enrichers.py
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType

# Globals to hold broadcast variables (set by init_lookup)
bc_municipality_coords = None
bc_municipality_codes = None

def init_municipality_lookup(spark, csv_path: str = "data/municipality_codes_to_coordinates.csv"):
    """
    Load municipality CSV and broadcast coordinates and codes.
    This must be called once from the driver, after SparkSession is created.
    CSV expected columns: code, latitude, longitude
    """
    global bc_municipality_coords, bc_municipality_codes

    # Load CSV on driver
    df = pd.read_csv(csv_path)
    coords = df[["latitude", "longitude"]].to_numpy(dtype=float)
    codes = df["code"].astype(int).to_numpy()  # Convert to int

    # Broadcast to executors
    bc_municipality_coords = spark.sparkContext.broadcast(coords)
    bc_municipality_codes = spark.sparkContext.broadcast(codes)


@pandas_udf(IntegerType())
def add_dk_area_udf(lon_series: pd.Series) -> pd.Series:
    """
    Vectorised computation of DK area from longitude.
    Rule: 1 if lon < 11 else 2. Returns 0 for invalid values.
    """
    results = []
    for lon in lon_series:
        try:
            if pd.isnull(lon):
                results.append(0)  # Default value for null
            else:
                lon_val = float(lon)
                results.append(1 if lon_val < 11 else 2)
        except Exception:
            results.append(0)  # Default value for errors
    return pd.Series(results, dtype='int32')


@pandas_udf(IntegerType())
def add_municipality_code_udf(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
    """
    Vectorised nearest-neighbour lookup using broadcasted municipality coords and codes.
    Returns municipality code (int) or 0 for invalid/missing data.
    """
    global bc_municipality_coords, bc_municipality_codes
    # Defensive: if broadcasts are not initialised, return all zeros
    if bc_municipality_coords is None or bc_municipality_codes is None:
        return pd.Series([0] * len(lat_series), dtype='int32')

    coords = bc_municipality_coords.value
    codes = bc_municipality_codes.value

    results = []
    for lat, lon in zip(lat_series, lon_series):
        try:
            if pd.isnull(lat) or pd.isnull(lon):
                results.append(0)  # Default value for null
                continue
            point = np.array([float(lat), float(lon)])
            # squared Euclidean distance
            dists = np.sum((coords - point) ** 2, axis=1)
            idx = int(np.argmin(dists))
            results.append(int(codes[idx]))
        except Exception:
            results.append(0)  # Default value for errors

    return pd.Series(results, dtype='int32')