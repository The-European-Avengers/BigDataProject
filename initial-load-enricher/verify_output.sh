import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, pandas_udf
from pyspark.sql.types import IntegerType
import pandas as pd
import numpy as np

# Globals to hold broadcast variables
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
    codes = df["code"].astype(int).to_numpy()

    # Broadcast to executors
    bc_municipality_coords = spark.sparkContext.broadcast(coords)
    bc_municipality_codes = spark.sparkContext.broadcast(codes)

    print(f"✓ Loaded {len(codes)} municipality codes for lookup")


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
                results.append(0)
            else:
                lon_val = float(lon)
                results.append(1 if lon_val < 11 else 2)
        except Exception:
            results.append(0)
    return pd.Series(results, dtype='int32')


@pandas_udf(IntegerType())
def add_municipality_code_udf(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
    """
    Vectorised nearest-neighbour lookup using broadcasted municipality coords and codes.
    Returns municipality code (int) or 0 for invalid/missing data.
    """
    global bc_municipality_coords, bc_municipality_codes

    if bc_municipality_coords is None or bc_municipality_codes is None:
        return pd.Series([0] * len(lat_series), dtype='int32')

    coords = bc_municipality_coords.value
    codes = bc_municipality_codes.value

    results = []
    for lat, lon in zip(lat_series, lon_series):
        try:
            if pd.isnull(lat) or pd.isnull(lon):
                results.append(0)
                continue
            point = np.array([float(lat), float(lon)])
            # squared Euclidean distance
            dists = np.sum((coords - point) ** 2, axis=1)
            idx = int(np.argmin(dists))
            results.append(int(codes[idx]))
        except Exception:
            results.append(0)

    return pd.Series(results, dtype='int32')


def process_weather_data(spark, hdfs_namenode, weather_type, value_column):
    """
    Process a specific weather type (wind, temp, or sun)

    Args:
        spark: SparkSession
        hdfs_namenode: HDFS namenode URI
        weather_type: 'weather-wind', 'weather-temp', or 'weather-sun'
        value_column: Column name containing the value (e.g., 'mean_wind_speed')
    """
    input_path = f"{hdfs_namenode}/raw/initial-load/{weather_type}/*.csv"

    print(f"\n{'='*60}")
    print(f"Processing {weather_type}")
    print(f"Input path: {input_path}")
    print(f"Value column: {value_column}")
    print(f"{'='*60}\n")

    # Read all CSV files for this weather type
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )

    print(f"✓ Loaded {df.count()} records from {weather_type}")

    # Parse timestamp and extract year/month
    df = df.withColumn("timeObserved", col("timeObserved").cast("timestamp"))
    df = df.withColumn("year", year(col("timeObserved")))
    df = df.withColumn("month", month(col("timeObserved")))

    # Add enrichment columns
    df_enriched = (
        df
        .withColumn("dkArea", add_dk_area_udf(col("lon")))
        .withColumn("municipalityCode", add_municipality_code_udf(col("lat"), col("lon")))
    )

    # Select final columns (keeping all original columns + new ones)
    df_final = df_enriched.select(
        "timeObserved",
        "stationId",
        "stationName",
        col(value_column),
        "lon",
        "lat",
        "dkArea",
        "municipalityCode",
        "year",
        "month"
    )

    # Get unique year-month combinations
    year_months = df_final.select("year", "month").distinct().collect()

    print(f"Found {len(year_months)} unique year-month combinations to process")

    # Process each year-month combination
    for row in year_months:
        year_val = row["year"]
        month_val = row["month"]

        # Filter data for this year-month
        partition_df = df_final.filter(
            (col("year") == year_val) & (col("month") == month_val)
        ).drop("year", "month")  # Drop the partition columns from final output

        # Output path: /historical/YYYY/weather-type/MM.avro
        output_path = f"{hdfs_namenode}/historical/{year_val}/{weather_type}/{month_val:02d}.avro"

        record_count = partition_df.count()
        print(f"  Writing {record_count} records to {output_path}")

        # Write as AVRO with overwrite mode
        partition_df.write.mode("overwrite").format("avro").save(output_path)

        print(f"  ✓ Successfully wrote {year_val}-{month_val:02d}")

    print(f"\n✓ Completed processing {weather_type}\n")


def main():
    print("="*60)
    print("BATCH ENRICHMENT JOB - Historical Weather Data")
    print("="*60)

    # Configuration
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000")
    municipality_csv = os.getenv("MUNICIPALITY_CSV", "data/municipality_codes_to_coordinates.csv")

    print(f"\nConfiguration:")
    print(f"  HDFS Namenode: {hdfs_namenode}")
    print(f"  Municipality CSV: {municipality_csv}")
    print()

    # Create Spark Session
    spark = (
        SparkSession.builder
        .appName("BatchWeatherEnrichment")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.defaultFS", hdfs_namenode)
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Initialize municipality lookup
    if os.path.exists(municipality_csv):
        init_municipality_lookup(spark, municipality_csv)
    else:
        print(f"ERROR: Municipality CSV not found at {municipality_csv}")
        sys.exit(1)

    try:
        # Process each weather type
        weather_configs = [
            ("weather-wind", "mean_wind_speed"),
            ("weather-temp", "mean_temp"),
            ("weather-sun", "mean_radiation")
        ]

        for weather_type, value_column in weather_configs:
            try:
                process_weather_data(spark, hdfs_namenode, weather_type, value_column)
            except Exception as e:
                print(f"✗ ERROR processing {weather_type}: {e}")
                import traceback
                traceback.print_exc()
                # Continue with next weather type instead of failing entire job

        print("\n" + "="*60)
        print("BATCH ENRICHMENT COMPLETED SUCCESSFULLY")
        print("="*60)

    except Exception as e:
        print(f"\n✗ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()