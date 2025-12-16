import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, to_timestamp, year
from pyspark.sql.types import IntegerType, DoubleType, TimestampType


def process_price_data(spark, hdfs_namenode):
    """
    Process electricity price data:
    - Read from /raw/initial-load/price/*.csv (DK1 and DK2 files)
    - Extract timestamp from MTU (UTC) field (first timestamp in range)
    - Extract dkArea from Area field (1 or 2)
    - Rename price column
    - Merge DK1 and DK2 data for each year
    - Deduplicate by (timestamp, dkArea)
    - Write to /historical/{YEAR}/price.avro (one file per year)
    """
    input_path = f"{hdfs_namenode}/raw/initial-load/price/*.csv"

    print(f"\n{'=' * 80}")
    print(f"Processing Electricity Price Data")
    print(f"Input path: {input_path}")
    print(f"{'=' * 80}\n")

    # Read all CSV files for price
    print(f"📖 [1/6] Reading CSV files from HDFS...")
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

    # Show sample of raw data (using original column names)
    print(f"\n📊 Sample raw data:")
    df.select("MTU (UTC)", "Area", "Day-ahead Price (EUR/MWh)").show(5, truncate=False)

    # Extract timestamp from MTU (UTC) field
    print(f"\n⏰ [2/6] Extracting timestamp from MTU (UTC) field...")
    start_time = datetime.now()

    # Extract first timestamp: "31/12/2020 23:00:00 - 01/01/2021 00:00:00" -> "31/12/2020 23:00:00"
    df = df.withColumn(
        "timestamp_str",
        regexp_extract(col("MTU (UTC)"), r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})", 1)
    )

    # Convert to timestamp (format: dd/MM/yyyy HH:mm:ss)
    df = df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp_str"), "dd/MM/yyyy HH:mm:ss")
    )

    # Drop the intermediate string column
    df = df.drop("timestamp_str")

    extract_time = (datetime.now() - start_time).total_seconds()
    print(f"✓ Timestamp extraction complete in {extract_time:.1f}s")

    # Extract dkArea from Area field
    print(f"\n🌍 [3/6] Extracting dkArea from Area field...")
    start_time = datetime.now()

    # Extract DK area number: "BZN|DK1" -> 1, "BZN|DK2" -> 2
    df = df.withColumn(
        "dkArea",
        regexp_extract(col("Area"), r"DK(\d)", 1).cast(IntegerType())
    )

    area_time = (datetime.now() - start_time).total_seconds()
    print(f"✓ DK area extraction complete in {area_time:.1f}s")

    # Rename price column
    print(f"\n📝 [4/6] Renaming price column...")
    start_time = datetime.now()

    # Rename from original name to new name, then cast to double
    df = df.withColumnRenamed("Day-ahead Price (EUR/MWh)", "price_EUR_MWh")
    df = df.withColumn("price_EUR_MWh", col("price_EUR_MWh").cast(DoubleType()))

    rename_time = (datetime.now() - start_time).total_seconds()
    print(f"✓ Price column renamed in {rename_time:.1f}s")

    # Select final columns and add year for partitioning
    print(f"\n📋 [5/6] Selecting final columns and extracting year...")
    start_time = datetime.now()

    df = df.select(
        col("timestamp"),
        col("dkArea"),
        col("price_EUR_MWh")
    )

    # Add year column for grouping
    df = df.withColumn("year", year(col("timestamp")))

    select_time = (datetime.now() - start_time).total_seconds()
    print(f"✓ Columns selected in {select_time:.1f}s")

    # Show processed schema
    print(f"\n📋 Processed schema:")
    df.printSchema()

    # Show sample of processed data
    print(f"\n📊 Sample processed data:")
    df.show(10, truncate=False)

    # Get unique years
    years = df.select("year").distinct().orderBy("year").collect()
    years_list = [row["year"] for row in years]
    print(f"\n✓ Found {len(years_list)} unique years to process: {years_list}")

    # Process each year
    print(f"\n💾 [6/6] Writing AVRO files to HDFS (with deduplication)...")
    total_start = datetime.now()

    for idx, year_val in enumerate(years_list, 1):
        # Filter data for this year
        year_df = df.filter(col("year") == year_val).drop("year")

        # Count records before deduplication
        before_count = year_df.count()

        # Deduplicate by (timestamp, dkArea)
        year_df_dedup = year_df.dropDuplicates(["timestamp", "dkArea"])

        # Count records after deduplication
        after_count = year_df_dedup.count()
        duplicates_removed = before_count - after_count

        # Output path: /historical/{YEAR}/price.avro
        output_path = f"{hdfs_namenode}/historical/{year_val}/price.avro"

        # Progress indicator
        print(f"  [{idx}/{len(years_list)}] Processing year {year_val}:")
        print(f"      Records before dedup: {before_count:,}")
        print(f"      Records after dedup:  {after_count:,}")
        if duplicates_removed > 0:
            print(f"      Duplicates removed:   {duplicates_removed:,}")

        write_start = datetime.now()

        # Write as AVRO with overwrite mode
        year_df_dedup.write.mode("overwrite").format("avro").save(output_path)

        write_time = (datetime.now() - write_start).total_seconds()
        print(f"      ✓ Written in {write_time:.1f}s → {output_path}")
        print()

    total_time = (datetime.now() - total_start).total_seconds()
    print(f"✓ All {len(years_list)} files written in {total_time:.1f}s")
    print(f"✅ Completed processing price data\n")


def main():
    print("=" * 80)
    print("BATCH ENRICHMENT JOB - Historical Electricity Price Data")
    print("=" * 80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Configuration
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000")

    print(f"\nConfiguration:")
    print(f"  HDFS Namenode: {hdfs_namenode}")
    print()

    # Create Spark Session
    print("🔧 Initializing Spark session...")
    spark = (
        SparkSession.builder
        .appName("BatchPriceEnrichment")
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

    overall_start = datetime.now()

    try:
        process_price_data(spark, hdfs_namenode)

        overall_time = (datetime.now() - overall_start).total_seconds()
        minutes = int(overall_time // 60)
        seconds = int(overall_time % 60)

        print("\n" + "=" * 80)
        print("✅ BATCH ENRICHMENT COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print(f"Total processing time: {minutes}m {seconds}s")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

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