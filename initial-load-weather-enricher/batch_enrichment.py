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

    print(f"📂 Loading municipality data from: {csv_path}")

    # Load CSV on driver
    df = pd.read_csv(csv_path)
    coords = df[["latitude", "longitude"]].to_numpy(dtype=float)
    codes = df["code"].astype(int).to_numpy()

    # Broadcast to executors
    bc_municipality_coords = spark.sparkContext.broadcast(coords)
    bc_municipality_codes = spark.sparkContext.broadcast(codes)

    print(f"✓ Loaded {len(codes)} municipality codes for lookup")
    print(f"  Sample codes: {codes[:10].tolist()}")


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

    print(f"\n{'=' * 60}")
    print(f"Processing {weather_type}")
    print(f"Input path: {input_path}")
    print(f"Value column: {value_column}")
    print(f"{'=' * 60}\n")

    # Read all CSV files for this weather type
    print(f"📖 [1/5] Reading CSV files from HDFS...")
    start_time = datetime.now()
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )

    record_count = df.count()
    read_time = (datetime.now() - start_time).total_seconds()
    print(f"✓ Loaded {record_count:,} records in {read_time:.1f}s")

    # Parse timestamp and extract year/month
    print(f"\n⏰ [2/5] Parsing timestamps and extracting year/month...")
    start_time = datetime.now()
    df = df.withColumn("timeObserved", col("timeObserved").cast("timestamp"))
    df = df.withColumn("year", year(col("timeObserved")))
    df = df.withColumn("month", month(col("timeObserved")))
    parse_time = (datetime.now() - start_time).total_seconds()
    print(f"✓ Timestamp parsing complete in {parse_time:.1f}s")

    # Add enrichment columns
    print(f"\n🔧 [3/5] Applying enrichment (dkArea + municipalityCode)...")
    start_time = datetime.now()
    df_enriched = (
        df
        .withColumn("dkArea", add_dk_area_udf(col("lon")))
        .withColumn("municipalityCode", add_municipality_code_udf(col("lat"), col("lon")))
    )
    enrich_time = (datetime.now() - start_time).total_seconds()
    print(f"✓ Enrichment applied in {enrich_time:.1f}s")

    # Select final columns (keeping all original columns + new ones)
    print(f"\n📋 [4/5] Selecting final columns...")
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
    year_months = df_final.select("year", "month").distinct().orderBy("year", "month").collect()
    print(f"✓ Found {len(year_months)} unique year-month combinations to process")

    # Show year range
    years = sorted(set(row["year"] for row in year_months))
    print(f"  Years: {years[0]} - {years[-1]}")

    # Process each year-month combination
    print(f"\n💾 [5/5] Writing AVRO files to HDFS...")
    total_start = datetime.now()

    for idx, row in enumerate(year_months, 1):
        year_val = row["year"]
        month_val = row["month"]

        # Filter data for this year-month
        partition_df = df_final.filter(
            (col("year") == year_val) & (col("month") == month_val)
        ).drop("year", "month")  # Drop the partition columns from final output

        # Output path: /historical/YYYY/weather-type/MM.avro
        output_path = f"{hdfs_namenode}/historical/{year_val}/{weather_type}/{month_val:02d}.avro"

        record_count = partition_df.count()

        # Progress indicator
        print(f"  [{idx}/{len(year_months)}] Writing {year_val}-{month_val:02d}: {record_count:,} records...")

        write_start = datetime.now()

        # Write as AVRO with overwrite mode
        partition_df.write.mode("overwrite").format("avro").save(output_path)

        write_time = (datetime.now() - write_start).total_seconds()
        print(f"      ✓ Completed in {write_time:.1f}s → {output_path}")

    total_time = (datetime.now() - total_start).total_seconds()
    print(f"\n✓ All {len(year_months)} files written in {total_time:.1f}s")
    print(f"✓ Completed processing {weather_type}\n")


def main():
    print("=" * 60)
    print("BATCH ENRICHMENT JOB - Historical Weather Data")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Configuration
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000")
    municipality_csv = os.getenv("MUNICIPALITY_CSV", "data/municipality_codes_to_coordinates.csv")

    print(f"\nConfiguration:")
    print(f"  HDFS Namenode: {hdfs_namenode}")
    print(f"  Municipality CSV: {municipality_csv}")
    print()

    # Create Spark Session
    print("🔧 Initializing Spark session...")
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
    print(f"✓ Spark session created (version {spark.version})")

    # Initialize municipality lookup
    if os.path.exists(municipality_csv):
        init_municipality_lookup(spark, municipality_csv)
    else:
        print(f"❌ ERROR: Municipality CSV not found at {municipality_csv}")
        sys.exit(1)

    overall_start = datetime.now()

    try:
        # Process each weather type
        weather_configs = [
            ("weather-wind", "mean_wind_speed"),
            ("weather-temp", "mean_temp"),
            ("weather-sun", "mean_radiation")
        ]

        print(f"\n📊 Processing {len(weather_configs)} weather types...\n")

        for idx, (weather_type, value_column) in enumerate(weather_configs, 1):
            print(f"\n{'#' * 60}")
            print(f"# Weather Type {idx}/{len(weather_configs)}: {weather_type}")
            print(f"{'#' * 60}")

            type_start = datetime.now()

            try:
                process_weather_data(spark, hdfs_namenode, weather_type, value_column)

                type_time = (datetime.now() - type_start).total_seconds()
                print(f"✅ {weather_type} completed in {type_time:.1f}s")

            except Exception as e:
                print(f"❌ ERROR processing {weather_type}: {e}")
                import traceback
                traceback.print_exc()
                # Continue with next weather type instead of failing entire job

        overall_time = (datetime.now() - overall_start).total_seconds()
        minutes = int(overall_time // 60)
        seconds = int(overall_time % 60)

        print("\n" + "=" * 60)
        print("✅ BATCH ENRICHMENT COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"Total processing time: {minutes}m {seconds}s")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        print("\n🛑 Shutting down Spark session...")
        spark.stop()
        print("✓ Spark session stopped")


if __name__ == "__main__":
    main()