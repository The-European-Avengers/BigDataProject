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

    print(f"📂 Loading municipality data from HDFS: {csv_path}")

    try:
        # Load CSV from HDFS using Spark DataFrame
        spark_df = spark.read.csv(csv_path, header=True, inferSchema=True)

        # Convert to Pandas on driver
        df = spark_df.toPandas()

        # Validate required columns
        required_cols = ["code", "latitude", "longitude"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns in CSV: {missing_cols}. Found columns: {list(df.columns)}")

        coords = df[["latitude", "longitude"]].to_numpy(dtype=float)
        codes = df["code"].astype(int).to_numpy()

        # Broadcast to executors
        bc_municipality_coords = spark.sparkContext.broadcast(coords)
        bc_municipality_codes = spark.sparkContext.broadcast(codes)

        print(f"✓ Successfully loaded {len(codes)} municipality codes from HDFS")
        print(f"  Sample codes: {codes[:10].tolist()}")

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


def get_existing_months(spark, hdfs_namenode, weather_type):
    """
    Check which year-month combinations already exist in HDFS.
    Returns a set of (year, month) tuples.
    """
    existing_months = set()

    try:
        # Try to list the historical directory for this weather type
        years_path = f"{hdfs_namenode}/historical/"

        # Use Spark to check what exists
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jvm.java.net.URI(hdfs_namenode),
            hadoop_conf
        )

        # List years (2020, 2021, etc.)
        years_dir = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(years_path)
        if fs.exists(years_dir):
            year_statuses = fs.listStatus(years_dir)

            for year_status in year_statuses:
                year_name = year_status.getPath().getName()
                if year_name.isdigit():
                    year = int(year_name)

                    # Check months for this year
                    weather_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
                        f"{hdfs_namenode}/historical/{year}/{weather_type}"
                    )

                    if fs.exists(weather_path):
                        month_statuses = fs.listStatus(weather_path)

                        for month_status in month_statuses:
                            month_name = month_status.getPath().getName()
                            # Extract month from "01.avro", "02.avro", etc.
                            if month_name.endswith('.avro'):
                                month = int(month_name.split('.')[0])
                                existing_months.add((year, month))

        if existing_months:
            print(f"  Found {len(existing_months)} existing month(s) in HDFS")
            print(f"  Sample: {list(sorted(existing_months))[:5]}")
        else:
            print(f"  No existing data found - will process all months")

    except Exception as e:
        print(f"  Warning: Could not check existing data: {e}")
        print(f"  Will process all months to be safe")

    return existing_months


def process_weather_data(spark, hdfs_namenode, weather_type, value_column, skip_existing=True):
    """
    Process a specific weather type (wind, temp, or sun)

    Args:
        spark: SparkSession
        hdfs_namenode: HDFS namenode URI
        weather_type: 'weather-wind', 'weather-temp', or 'weather-sun'
        value_column: Column name containing the value (e.g., 'mean_wind_speed')
        skip_existing: If True, skip months that already exist in HDFS
    """
    input_path = f"{hdfs_namenode}/raw/initial-load/{weather_type}/*.csv"

    print(f"\n{'=' * 60}")
    print(f"Processing {weather_type}")
    print(f"Input path: {input_path}")
    print(f"Value column: {value_column}")
    print(f"Mode: {'Incremental (skip existing)' if skip_existing else 'Full reprocess'}")
    print(f"{'=' * 60}\n")

    # Check which months already exist
    existing_months = set()
    if skip_existing:
        print(f"🔍 Checking for existing processed data...")
        existing_months = get_existing_months(spark, hdfs_namenode, weather_type)

    # Read all CSV files for this weather type
    print(f"\n📖 [1/5] Reading CSV files from HDFS...")
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

    # Select final columns
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

    # Filter out existing months if skip_existing is True
    if skip_existing:
        original_count = len(year_months)
        year_months = [row for row in year_months
                       if (row["year"], row["month"]) not in existing_months]
        skipped_count = original_count - len(year_months)

        if skipped_count > 0:
            print(f"✓ Found {original_count} total year-month combinations")
            print(f"  → Skipping {skipped_count} existing month(s)")
            print(f"  → Processing {len(year_months)} new month(s)")
        else:
            print(f"✓ Found {len(year_months)} new year-month combinations to process")
    else:
        print(f"✓ Found {len(year_months)} year-month combinations to process")

    if not year_months:
        print(f"\n✅ No new data to process for {weather_type}")
        return

    # Show year range
    years = sorted(set(row["year"] for row in year_months))
    if years:
        print(f"  Years to process: {years[0]} - {years[-1]}")

    # Process each year-month combination
    print(f"\n💾 [5/5] Writing AVRO files to HDFS...")
    total_start = datetime.now()

    for idx, row in enumerate(year_months, 1):
        year_val = row["year"]
        month_val = row["month"]

        # Filter data for this year-month
        partition_df = df_final.filter(
            (col("year") == year_val) & (col("month") == month_val)
        ).drop("year", "month")

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
    print(f"\n✓ All {len(year_months)} new file(s) written in {total_time:.1f}s")
    print(f"✓ Completed processing {weather_type}\n")


def main():
    print("=" * 60)
    print("BATCH ENRICHMENT JOB - Historical Weather Data")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Configuration
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000")
    municipality_csv_hdfs = os.getenv("MUNICIPALITY_CSV_HDFS",
                                      "hdfs://namenode-g5:9000/utils/municipality_codes_to_coordinates.csv")

    # NEW: Add environment variable to control incremental mode
    skip_existing = os.getenv("SKIP_EXISTING_MONTHS", "true").lower() == "true"

    print(f"\nConfiguration:")
    print(f"  HDFS Namenode: {hdfs_namenode}")
    print(f"  Municipality CSV (HDFS): {municipality_csv_hdfs}")
    print(f"  Skip Existing Months: {skip_existing}")
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

    # Initialize municipality lookup from HDFS
    init_municipality_lookup(spark, municipality_csv_hdfs)

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
                process_weather_data(spark, hdfs_namenode, weather_type, value_column,
                                     skip_existing=skip_existing)

                type_time = (datetime.now() - type_start).total_seconds()
                print(f"✅ {weather_type} completed in {type_time:.1f}s")

            except Exception as e:
                print(f"❌ ERROR processing {weather_type}: {e}")
                import traceback
                traceback.print_exc()

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