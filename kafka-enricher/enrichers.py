# enrichers.py
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

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
    codes = df["code"].astype(str).to_numpy()

    # Broadcast to executors
    bc_municipality_coords = spark.sparkContext.broadcast(coords)
    bc_municipality_codes = spark.sparkContext.broadcast(codes)


@pandas_udf(StringType())
def add_dk_area_udf(lon_series: pd.Series) -> pd.Series:
    """
    Vectorised computation of DK area from longitude.
    Rule: '1' if lon < 11 else '2'. Returns None for invalid values.
    """
    results = []
    for lon in lon_series:
        try:
            if pd.isnull(lon):
                results.append(None)
            else:
                lon_val = float(lon)
                results.append("1" if lon_val < 11 else "2")
        except Exception:
            results.append(None)
    return pd.Series(results)


@pandas_udf(StringType())
def add_municipality_code_udf(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
    """
    Vectorised nearest-neighbour lookup using broadcasted municipality coords and codes.
    Returns municipality code (string) or None.
    """
    global bc_municipality_coords, bc_municipality_codes
    # Defensive: if broadcasts are not initialised, return all None
    if bc_municipality_coords is None or bc_municipality_codes is None:
        return pd.Series([None] * len(lat_series))

    coords = bc_municipality_coords.value
    codes = bc_municipality_codes.value

    results = []
    for lat, lon in zip(lat_series, lon_series):
        try:
            if pd.isnull(lat) or pd.isnull(lon):
                results.append(None)
                continue
            point = np.array([float(lat), float(lon)])
            # squared Euclidean distance
            dists = np.sum((coords - point) ** 2, axis=1)
            idx = int(np.argmin(dists))
            results.append(str(codes[idx]))
        except Exception:
            results.append(None)

    return pd.Series(results)
