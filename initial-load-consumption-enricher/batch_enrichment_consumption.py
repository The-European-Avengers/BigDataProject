import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, pandas_udf
from pyspark.sql.types import IntegerType
import pandas as pd
import numpy as np

# Globals to hold broadcast variables
bc_municipality_to_coords = None


def init_municipality_lookup(spark,
                             csv_path: str = "hdfs://namenode-g5:9000/utils/municipality_codes_to_coordinates.csv"):
    """
    Load municipality CSV from HDFS and broadcast municipality code -> coordinates mapping.
    This must be called once from the driver, after SparkSession is created.
    CSV expected columns: code, name, latitude, longitude

    Note: Multiple rows can have the same municipality code (different coordinates).
    We'll take the first occurrence for each code.

    Args:
        spark: SparkSession instance
        csv_path: HDFS path to CSV file (default: hdfs://namenode-g5:9000/utils/municipality_codes_to_coordinates.csv)

    Raises:
        Exception: If file cannot be loaded from HDFS
    """
    global bc_municipality_to_coords

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

        # Create a mapping: municipality_code -> (latitude, longitude)
        # If there are duplicates, keep the first one
        municipality_map = {}
        for _, row in df.iterrows():
            code = int(row['code'])
            if code not in municipality_map:
                municipality_map[code] = (float(row['latitude']), float(row['longitude']))

        # Broadcast to executors
        bc_municipality_to_coords = spark.sparkContext.broadcast(municipality_map)

        print(f"✓ Successfully loaded {len(municipality_map)} unique municipality codes from HDFS")
        print(f"  Sample codes: {list(municipality_map.keys())[:10]}")

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
def add_dk_area_udf(municipality_code_series: pd.Series) -> pd.Series:
    """
    Calculate DK area from municipality code by looking up coordinates.
    Rule: 1 if lon < 11 else 2. Returns 0 for invalid/unmapped values.
    """
    global bc_municipality_to_coords

    if bc_municipality_to_coords is None:
        return pd.Series([0] * len(municipality_code_series), dtype='int32')

    municipality_map = bc_municipality_to_coords.value
    results = []

    for muni_code in municipality_code_series:
        try:
            if pd.isnull(muni_code):
                results.append(0)
                continue

            code = int(muni_code)

            # Look up coordinates for this municipality code
            if code in municipality_map:
                lat, lon = municipality_map[code]
                # Calculate dkArea: 1 if lon < 11 else 2
                dk_area = 1 if lon < 11 else 2
                results.append(dk_area)
            else:
                # Municipality code not found in lookup
                results.append(0)
        except Exception:
            results.append(0)

    return pd.Series(results, dtype='int32')


def process_consumption_data(spark, hdfs_namenode):
    """
    Process consumption data:
    - Read from /raw/initial-load/consumption/*.csv
    - Rename columns to camelCase
    - Add dkArea based on municipalityCode
    - Write to /historical/{YEAR}/consumption/{MONTH}.avro partitioned by year and month
    """
    input_path = f"{hdfs_namenode}/raw/initial-load/consumption/*.csv"

    print(f"\n{'=' * 60}")
    print(f"Processing Consumption Data")
    print(f"Input path: {input_path}")
    print(f"{'=' * 60}\n")

    # Read all CSV files for consumption
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

    # Show original schema
    print(f"\n📋 Original schema:")
    df.printSchema()

    # Rename columns to camelCase
    print(f"\n🔄 [2/5] Renaming columns to camelCase...")
    start_time = datetime.now()

    df = df.withColumnRenamed("ConsumptionkWh", "consumptionKwh") \
        .withColumnRenamed("HeatingCategory", "heatingCategory") \
        .withColumnRenamed("HousingCategory", "housingCategory") \
        .withColumnRenamed("Municipality", "municipality") \
        .withColumnRenamed("MunicipalityCode", "municipalityCode") \
        .withColumnRenamed("RegionName", "regionName") \
        .withColumnRenamed("TimeDK", "timeDK") \
        .withColumnRenamed("TimeUTC", "timeUTC")

    rename_time = (datetime.now() - start_time).total_seconds()
    print(f"✓ Columns renamed in {rename_time:.1f}s")

    # Show renamed schema
    print(f"\n📋 Renamed schema:")
    df.printSchema()

    # Parse timestamp and extract year/month
    print(f"\n⏰ [3/5] Parsing timestamps and extracting year/month...")
    start_time = datetime.now()
    df = df.withColumn("timeUTC", col("timeUTC").cast("timestamp"))
    df = df.withColumn("year", year(col("timeUTC")))
    df = df.withColumn("month", month(col("timeUTC")))
    parse_time = (datetime.now() - start_time).total_seconds()
    print(f"✓ Timestamp parsing complete in {parse_time:.1f}s")

    # Add enrichment column: dkArea
    print(f"\n🔧 [4/5] Applying enrichment (dkArea based on municipalityCode)...")
    start_time = datetime.now()
    df_enriched = df.withColumn("dkArea", add_dk_area_udf(col("municipalityCode")))
    enrich_time = (datetime.now() - start_time).total_seconds()
    print(f"✓ Enrichment applied in {enrich_time:.1f}s")

    # Show enriched schema
    print(f"\n📋 Enriched schema:")
    df_enriched.printSchema()

    # Select final columns
    print(f"\n📋 Selecting final columns...")
    df_final = df_enriched.select(
        "consumptionKwh",
        "heatingCategory",
        "housingCategory",
        "municipality",
        "municipalityCode",
        "regionName",
        "timeDK",
        "timeUTC",
        "dkArea",
        "year",
        "month"
    )

    # Get unique year-month combinations
    year_months = df_final.select("year", "month").distinct().orderBy("year", "month").collect()
    print(f"✓ Found {len(year_months)} unique year-month combinations to process")

    # Show year range
    years = sorted(set(row["year"] for row in year_months))
    months_range = sorted(set(row["month"] for row in year_months))
    print(f"  Years: {years[0]} - {years[-1]}")
    print(f"  Months: {months_range[0]} - {months_range[-1]}")

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

        # Output path: /historical/{YEAR}/consumption/{MM}.avro
        output_path = f"{hdfs_namenode}/historical/{year_val}/consumption/{month_val:02d}.avro"

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
    print(f"✅ Completed processing consumption data\n")


def main():
    print("=" * 60)
    print("BATCH ENRICHMENT JOB - Historical Consumption Data")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Configuration
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000")
    municipality_csv_hdfs = os.getenv("MUNICIPALITY_CSV_HDFS",
                                      "hdfs://namenode-g5:9000/utils/municipality_codes_to_coordinates.csv")

    print(f"\nConfiguration:")
    print(f"  HDFS Namenode: {hdfs_namenode}")
    print(f"  Municipality CSV (HDFS): {municipality_csv_hdfs}")
    print()

    # Create Spark Session
    print("🔧 Initializing Spark session...")
    spark = (
        SparkSession.builder
        .appName("BatchConsumptionEnrichment")
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
        process_consumption_data(spark, hdfs_namenode)

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