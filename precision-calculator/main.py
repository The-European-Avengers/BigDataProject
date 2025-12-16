import os
import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# Import our modules
from src.data_reader import PrecisionDataReader
from src.precision_calculator import PrecisionCalculator
from src.data_writer import PrecisionDataWriter


def init_spark_session(hdfs_namenode):
    """Initialize Spark session with proper configuration."""
    print("🔧 Initializing Spark session...")
    spark = (
        SparkSession.builder
        .appName("BatchPrecisionCalculation")
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
    return spark


def get_last_processed_timestamp(spark, hdfs_namenode):
    """
    Retrieve the last processed timestamp from HDFS.
    Returns None if file doesn't exist (first run).
    """
    state_path = f"{hdfs_namenode}/utils/precision_state.txt"
    
    try:
        # Try to read the state file
        df = spark.read.text(state_path)
        timestamp_str = df.first()[0]
        last_timestamp = datetime.fromisoformat(timestamp_str)
        print(f"📅 Last processed timestamp: {last_timestamp}")
        return last_timestamp
    except Exception:
        print(f"📅 No previous state found - this is the first run")
        return None


def save_last_processed_timestamp(spark, hdfs_namenode, timestamp):
    """Save the last processed timestamp to HDFS."""
    state_path = f"{hdfs_namenode}/utils/precision_state.txt"
    
    try:
        # Create a DataFrame with single row containing timestamp
        timestamp_str = timestamp.isoformat()
        df = spark.createDataFrame([(timestamp_str,)], ["timestamp"])
        
        # Write to HDFS (overwrite mode)
        df.write.mode("overwrite").text(state_path)
        print(f"✓ Saved last processed timestamp: {timestamp}")
    except Exception as e:
        print(f"⚠️  Warning: Could not save state file: {e}")


def process_precision_data(spark, hdfs_namenode, last_timestamp=None):
    """
    Main processing function that orchestrates reading, calculating, and writing.
    
    Args:
        spark: SparkSession
        hdfs_namenode: HDFS namenode URL
        last_timestamp: Last processed timestamp (None for first run)
    
    Returns:
        datetime: The maximum timestamp processed in this run, or None if no data processed
    """
    print(f"\n{'=' * 60}")
    print(f"Processing Prediction Precision Data")
    if last_timestamp:
        print(f"Processing data after: {last_timestamp}")
    else:
        print(f"Processing all available data (first run)")
    print(f"{'=' * 60}\n")
    
    overall_start = datetime.now()
    
    # Initialize components
    reader = PrecisionDataReader(spark, hdfs_namenode)
    calculator = PrecisionCalculator(spark, hdfs_namenode)
    writer = PrecisionDataWriter(spark, hdfs_namenode)
    
    # Step 1: Read prediction files
    print(f"📖 [Step 1/5] Reading prediction data from HDFS...")
    step_start = datetime.now()
    predictions_df = reader.read_prediction_files(last_timestamp)
    
    if predictions_df.rdd.isEmpty():
        print(f"ℹ️  No new prediction data to process")
        return last_timestamp
    
    record_count = predictions_df.count()
    step_time = (datetime.now() - step_start).total_seconds()
    print(f"✓ Loaded {record_count:,} prediction records in {step_time:.1f}s\n")
    
    # Add year and month columns for grouping
    predictions_df = predictions_df.withColumn("year", year(col("timestamp")))
    predictions_df = predictions_df.withColumn("month", month(col("timestamp")))
    
    # Get unique year-month combinations
    year_months = predictions_df.select("year", "month").distinct().orderBy("year", "month").collect()
    
    print(f"📖 [Step 2/5] Loading real data for {len(year_months)} year-month combinations...")
    step_start = datetime.now()
    
    # Load real data for each year-month
    all_results = []
    
    for idx, row in enumerate(year_months, 1):
        year_val = row["year"]
        month_val = row["month"]
        
        print(f"  [{idx}/{len(year_months)}] Processing {year_val}-{month_val:02d}...")
        
        # Filter predictions for this year-month
        partition_predictions = predictions_df.filter(
            (col("year") == year_val) & (col("month") == month_val)
        ).drop("year", "month")
        
        # Read real consumption data
        real_consumption = reader.read_real_consumption(year_val, month_val)
        
        # Read real price data
        real_price = reader.read_real_price(year_val)
        
        # Check if we have any real data
        if real_consumption is None and real_price is None:
            print(f"    ⚠️  No real data available for {year_val}-{month_val:02d}, skipping...")
            continue
        
        # Calculate precision
        print(f"  🔧 [Step 3/5] Calculating precision for {year_val}-{month_val:02d}...")
        precision_data = calculator.calculate_precision(
            partition_predictions,
            real_consumption,
            real_price
        )
        
        all_results.append(precision_data)
    
    step_time = (datetime.now() - step_start).total_seconds()
    print(f"✓ Real data loaded and precision calculated in {step_time:.1f}s\n")
    
    # Check if we processed any data
    if not all_results:
        print(f"ℹ️  No real data available for any prediction records, skipping write")
        return last_timestamp
    
    # Union all results
    print(f"🔧 Combining results from all year-month partitions...")
    combined_results = all_results[0]
    for df in all_results[1:]:
        combined_results = combined_results.union(df)
    
    result_count = combined_results.count()
    print(f"✓ Combined {result_count:,} records with precision data\n")
    
    # Step 4: Write precision data
    print(f"💾 [Step 4/5] Writing precision data to HDFS...")
    step_start = datetime.now()
    max_timestamp = writer.write_precision_data(combined_results)
    step_time = (datetime.now() - step_start).total_seconds()
    print(f"✓ Precision data written in {step_time:.1f}s\n")
    
    # Step 5: Write precision summary
    print(f"📊 [Step 5/5] Writing precision summary statistics...")
    step_start = datetime.now()
    writer.write_precision_summary(combined_results)
    step_time = (datetime.now() - step_start).total_seconds()
    print(f"✓ Precision summary written in {step_time:.1f}s\n")
    
    overall_time = (datetime.now() - overall_start).total_seconds()
    minutes = int(overall_time // 60)
    seconds = int(overall_time % 60)
    
    print(f"✅ Processing completed successfully in {minutes}m {seconds}s")
    
    return max_timestamp


def main():
    print("=" * 60)
    print("BATCH PRECISION JOB - Prediction Precision Calculation")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Configuration
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000")
    sleep_interval = int(os.getenv("SLEEP_INTERVAL_HOURS", "6")) * 3600  # Default: 6 hours
    
    print(f"Configuration:")
    print(f"  HDFS Namenode: {hdfs_namenode}")
    print(f"  Sleep Interval: {sleep_interval // 3600} hours")
    print()
    
    # Initialize Spark
    print("🔧 Initializing Spark session...")
    sys.stdout.flush()
    spark = init_spark_session(hdfs_namenode)
    print("✓ Spark session initialized")
    sys.stdout.flush()
    
    # Test HDFS connectivity
    print("🔗 Testing HDFS connectivity...")
    sys.stdout.flush()
    try:
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jvm.java.net.URI(hdfs_namenode),
            hadoop_conf
        )
        print(f"✓ Successfully connected to HDFS at {hdfs_namenode}")
        sys.stdout.flush()
    except Exception as e:
        print(f"❌ Failed to connect to HDFS: {e}")
        sys.stdout.flush()
        sys.exit(1)
    
    try:
        while True:
            run_start = datetime.now()
            print(f"\n{'=' * 60}")
            print(f"Starting processing run at {run_start.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'=' * 60}\n")
            sys.stdout.flush()
            
            try:
                # Get last processed timestamp
                print("📅 Checking for previous state...")
                sys.stdout.flush()
                last_timestamp = get_last_processed_timestamp(spark, hdfs_namenode)
                print(f"✓ State check complete")
                sys.stdout.flush()
                
                # Process data
                print("🚀 Starting data processing...")
                sys.stdout.flush()
                max_timestamp = process_precision_data(spark, hdfs_namenode, last_timestamp)
                print("✓ Data processing complete")
                sys.stdout.flush()
                
                # Save the new last processed timestamp
                if max_timestamp and max_timestamp != last_timestamp:
                    save_last_processed_timestamp(spark, hdfs_namenode, max_timestamp)
                
                print(f"\n{'=' * 60}")
                print(f"✅ Run completed successfully")
                print(f"{'=' * 60}\n")
                
            except Exception as e:
                print(f"\n❌ ERROR during processing: {e}")
                import traceback
                traceback.print_exc()
                print(f"\n⚠️  Will retry in {sleep_interval // 3600} hours...\n")
            
            # Sleep until next run
            next_run = datetime.now().timestamp() + sleep_interval
            next_run_dt = datetime.fromtimestamp(next_run)
            
            print(f"😴 Sleeping until next run at {next_run_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   (waiting {sleep_interval // 3600} hours...)\n")
            
            time.sleep(sleep_interval)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Received interrupt signal - shutting down gracefully...")
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        print("\n🛑 Shutting down Spark session...")
        spark.stop()
        print("✓ Spark session stopped")
        print("\n" + "=" * 60)
        print("Job terminated")
        print("=" * 60)


if __name__ == "__main__":
    main()