# enrichers.py
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType

# Globals to hold broadcast variables (set by init_lookup)
bc_municipality_coords = None
bc_municipality_codes = None


def init_municipality_lookup(spark,
                             csv_path: str = "hdfs://namenode-g5:9000/utils/municipality_codes_to_coordinates.csv"):
    """
    Load municipality CSV from HDFS and broadcast coordinates and codes.
    This must be called once from the driver, after SparkSession is created.
    CSV expected columns: code, latitude, longitude

    Args:
        spark: SparkSession instance
        csv_path: HDFS path to CSV file (default: hdfs://namenode-g5:9000/utils/municipality_codes_to_coordinates.csv)

    Raises:
        Exception: If file cannot be loaded from HDFS
    """
    global bc_municipality_coords, bc_municipality_codes

    print(f"Loading municipality data from HDFS: {csv_path}")

    try:
        # Load CSV from HDFS using Spark DataFrame
        spark_df = spark.read.csv(csv_path, header=True, inferSchema=True)

        # Convert to Pandas on driver
        pdf = spark_df.toPandas()

        # Validate required columns
        required_cols = ["code", "latitude", "longitude"]
        missing_cols = [col for col in required_cols if col not in pdf.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns in CSV: {missing_cols}. Found columns: {list(pdf.columns)}")

        # Extract coordinates and codes
        coords = pdf[["latitude", "longitude"]].to_numpy(dtype=float)
        codes = pdf["code"].astype(int).to_numpy()

        # Validate data
        if len(codes) == 0:
            raise ValueError("CSV file is empty or contains no valid data")

        # Broadcast to executors
        bc_municipality_coords = spark.sparkContext.broadcast(coords)
        bc_municipality_codes = spark.sparkContext.broadcast(codes)

        print(f"✓ Successfully loaded {len(codes)} municipalities from HDFS")
        print(f"✓ Municipality lookup initialized and broadcast to executors")

    except Exception as e:
        print("=" * 80)
        print("ERROR: Failed to load municipality data from HDFS")
        print("=" * 80)
        print(f"Path attempted: {csv_path}")
        print(f"Error: {e}")
        print("")
        print("To fix this issue:")
        print("1. Make sure the file exists in HDFS:")
        print(f"   kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -ls /utils/")
        print("")
        print("2. If the file doesn't exist, upload it:")
        print("   kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -mkdir -p /utils")
        print("   kubectl cp municipality_codes_to_coordinates.csv bd-bd-gr-05/namenode-g5-0:/tmp/")
        print(
            "   kubectl exec -it namenode-g5-0 -n bd-bd-gr-05 -- hdfs dfs -put /tmp/municipality_codes_to_coordinates.csv /utils/")
        print("=" * 80)
        raise e


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