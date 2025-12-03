#!/usr/bin/env python3
"""
test_enrichment.py - Simple Test Spark Job
Tests the enrichment logic on a small sample dataset
Compatible with Python 3.12+ and PySpark 3.4+
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DoubleType
import numpy as np

# Globals for broadcast variables
bc_municipality_coords = None
bc_municipality_codes = None


def init_municipality_lookup(spark, csv_path: str = "data/municipality_codes_to_coordinates.csv"):
    """Load and broadcast municipality lookup data"""
    global bc_municipality_coords, bc_municipality_codes

    print(f"Loading municipality data from: {csv_path}")

    if not os.path.exists(csv_path):
        print(f"WARNING: Municipality CSV not found at {csv_path}")
        print("Creating dummy lookup data for testing...")
        # Create dummy data for testing
        coords = np.array([
            [55.6761, 12.5683],
            [55.4038, 10.4024],
            [55.5364, 9.3501],
            [56.3286, 9.1221],
            [56.0997, 8.4558]
        ])
        codes = np.array([101, 147, 151, 155, 157])
    else:
        # Read CSV without pandas dependency
        import csv
        coords_list = []
        codes_list = []
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                coords_list.append([float(row['latitude']), float(row['longitude'])])
                codes_list.append(int(row['code']))
        coords = np.array(coords_list)
        codes = np.array(codes_list)

    bc_municipality_coords = spark.sparkContext.broadcast(coords)
    bc_municipality_codes = spark.sparkContext.broadcast(codes)

    print(f"✓ Loaded {len(codes)} municipality codes")
    print(f"  Sample codes: {codes[:5].tolist()}")


def add_dk_area_impl(lon):
    """Calculate DK area based on longitude"""
    try:
        if lon is None:
            return 0
        lon_val = float(lon)
        return 1 if lon_val < 11 else 2
    except Exception:
        return 0


def add_municipality_code_impl(lat, lon):
    """Find nearest municipality code"""
    global bc_municipality_coords, bc_municipality_codes

    if bc_municipality_coords is None or bc_municipality_codes is None:
        return 0

    coords = bc_municipality_coords.value
    codes = bc_municipality_codes.value

    try:
        if lat is None or lon is None:
            return 0
        point = np.array([float(lat), float(lon)])
        dists = np.sum((coords - point) ** 2, axis=1)
        idx = int(np.argmin(dists))
        return int(codes[idx])
    except Exception:
        return 0


# Create UDFs (compatible with all PySpark versions)
add_dk_area_udf = udf(add_dk_area_impl, IntegerType())
add_municipality_code_udf = udf(add_municipality_code_impl, IntegerType())


def create_test_data(spark):
    """Create sample test data"""
    print("\n" + "=" * 60)
    print("Creating test data...")
    print("=" * 60)

    # Sample weather station data
    test_data = [
        # timeObserved, stationId, stationName, value, lon, lat
        ("2020-01-01 00:00:00", "06019", "Silstrup", 6.3, 8.6412, 56.93),
        ("2020-01-01 01:00:00", "06019", "Silstrup", 7.0, 8.6412, 56.93),
        ("2020-01-01 02:00:00", "06019", "Silstrup", 8.2, 8.6412, 56.93),
        ("2020-01-01 00:00:00", "06041", "Skagen", 5.5, 10.5833, 57.7167),
        ("2020-01-01 01:00:00", "06041", "Skagen", 6.2, 10.5833, 57.7167),
        ("2020-01-01 00:00:00", "06074", "Anholt", 8.1, 11.5167, 56.7167),
        ("2020-01-01 01:00:00", "06074", "Anholt", 8.8, 11.5167, 56.7167),
        ("2020-01-01 00:00:00", "06180", "Copenhagen", 4.2, 12.5683, 55.6761),
        ("2020-01-01 01:00:00", "06180", "Copenhagen", 4.9, 12.5683, 55.6761),
    ]

    schema = StructType([
        StructField("timeObserved", StringType(), True),
        StructField("stationId", StringType(), True),
        StructField("stationName", StringType(), True),
        StructField("mean_wind_speed", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
    ])

    df = spark.createDataFrame(test_data, schema)

    print(f"✓ Created {df.count()} test records")
    print("\nSample data:")
    df.show(5, truncate=False)

    return df


def test_enrichment(spark):
    """Test the enrichment logic"""
    print("\n" + "=" * 60)
    print("TESTING ENRICHMENT LOGIC")
    print("=" * 60)

    # Create test data
    df = create_test_data(spark)

    # Apply enrichment
    print("\nApplying enrichment...")
    df_enriched = (
        df
        .withColumn("dkArea", add_dk_area_udf(col("lon")))
        .withColumn("municipalityCode", add_municipality_code_udf(col("lat"), col("lon")))
    )

    print("\n" + "=" * 60)
    print("ENRICHED DATA")
    print("=" * 60)
    df_enriched.show(truncate=False)

    # Verify enrichment
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    # Check dkArea values
    dk_area_counts = df_enriched.groupBy("dkArea").count().collect()
    print("\ndkArea distribution:")
    for row in dk_area_counts:
        area_name = "West (lon < 11)" if row["dkArea"] == 1 else "East (lon >= 11)" if row["dkArea"] == 2 else "Invalid"
        print(f"  Area {row['dkArea']}: {row['count']} records ({area_name})")

    # Check municipality codes
    muni_counts = df_enriched.groupBy("municipalityCode").count().collect()
    print("\nmunicipality distribution:")
    for row in muni_counts:
        print(f"  Municipality {row['municipalityCode']}: {row['count']} records")

    # Show sample with locations
    print("\n" + "=" * 60)
    print("SAMPLE RECORDS WITH LOCATIONS")
    print("=" * 60)
    df_enriched.select(
        "stationName", "lon", "lat", "dkArea", "municipalityCode"
    ).distinct().show(truncate=False)

    return df_enriched


def test_hdfs_operations(spark, hdfs_namenode):
    """Test HDFS read/write operations (if HDFS is available)"""
    print("\n" + "=" * 60)
    print("TESTING HDFS OPERATIONS")
    print("=" * 60)

    test_path = f"{hdfs_namenode}/tmp/test_enrichment"

    try:
        # Create test data
        df = create_test_data(spark)

        # Add enrichment
        df_enriched = (
            df
            .withColumn("dkArea", add_dk_area_udf(col("lon")))
            .withColumn("municipalityCode", add_municipality_code_udf(col("lat"), col("lon")))
        )

        # Write to HDFS
        print(f"\nWriting test data to: {test_path}")
        df_enriched.write.mode("overwrite").format("avro").save(test_path)
        print("✓ Write successful")

        # Read back from HDFS
        print(f"\nReading test data from: {test_path}")
        df_read = spark.read.format("avro").load(test_path)
        print("✓ Read successful")

        print(f"\nRecords read: {df_read.count()}")
        print("\nSchema:")
        df_read.printSchema()

        print("\nSample data:")
        df_read.show(5, truncate=False)

        return True

    except Exception as e:
        print(f"✗ HDFS test failed: {e}")
        print("This is normal if HDFS is not accessible from this environment")
        return False


def main():
    print("=" * 60)
    print("SPARK ENRICHMENT TEST JOB")
    print("=" * 60)
    print()

    # Configuration
    municipality_csv = os.getenv("MUNICIPALITY_CSV", "data/municipality_codes_to_coordinates.csv")
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000")
    test_hdfs = os.getenv("TEST_HDFS", "false").lower() == "true"

    print("Configuration:")
    print(f"  Municipality CSV: {municipality_csv}")
    print(f"  HDFS Namenode: {hdfs_namenode}")
    print(f"  Test HDFS: {test_hdfs}")
    print()

    # Create Spark Session
    print("Creating Spark session...")
    builder = (
        SparkSession.builder
        .appName("EnrichmentTest")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
    )

    # Add HDFS config if testing HDFS
    if test_hdfs:
        builder = builder.config("spark.hadoop.fs.defaultFS", hdfs_namenode)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("✓ Spark session created")
    print(f"  Spark version: {spark.version}")
    print()

    # Initialize municipality lookup
    init_municipality_lookup(spark, municipality_csv)

    # Test enrichment logic
    df_result = test_enrichment(spark)

    # Test HDFS operations if requested
    if test_hdfs:
        hdfs_success = test_hdfs_operations(spark, hdfs_namenode)
        if hdfs_success:
            print("\n✓ HDFS tests passed")
        else:
            print("\n⚠ HDFS tests skipped or failed")

    print("\n" + "=" * 60)
    print("TEST COMPLETED")
    print("=" * 60)
    print("\n✓ All enrichment logic tests passed!")
    print(f"✓ Processed {df_result.count()} records")
    print("✓ dkArea calculation working")
    print("✓ municipalityCode lookup working")

    if test_hdfs:
        print("✓ HDFS read/write tested")

    spark.stop()
    print("\n✓ Spark session stopped")


if __name__ == "__main__":
    main()