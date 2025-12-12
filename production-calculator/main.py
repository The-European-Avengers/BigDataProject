import os
import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession

# Import our modules
from src.data_reader import ProductionDataReader
from src.production_calculator import ProductionCalculator
from src.data_writer import ProductionDataWriter


def init_spark_session(hdfs_namenode):
    """Initialize Spark session with proper configuration."""
    print("🔧 Initializing Spark session...")
    spark = (
        SparkSession.builder
        .appName("BatchProductionCalculation")
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
    state_path = f"{hdfs_namenode}/utils/production_job_state.txt"
    
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
    state_path = f"{hdfs_namenode}/utils/production_job_state.txt"
    
    try:
        # Create a DataFrame with single row containing timestamp
        timestamp_str = timestamp.isoformat()
        df = spark.createDataFrame([(timestamp_str,)], ["timestamp"])
        
        # Write to HDFS (overwrite mode)
        df.write.mode("overwrite").text(state_path)
        print(f"✓ Saved last processed timestamp: {timestamp}")
    except Exception as e:
        print(f"⚠️  Warning: Could not save state file: {e}")


def process_production_data(spark, hdfs_namenode, last_timestamp=None):
    """
    Main processing function that orchestrates reading, calculating, and writing.
    
    Args:
        spark: SparkSession
        hdfs_namenode: HDFS namenode URL
        last_timestamp: Last processed timestamp (None for first run)
    
    Returns:
        datetime: The maximum timestamp processed in this run
    """
    print(f"\n{'=' * 60}")
    print(f"Processing Green Energy Production Data")
    if last_timestamp:
        print(f"Processing data after: {last_timestamp}")
    else:
        print(f"Processing all available data (first run)")
    print(f"{'=' * 60}\n")
    
    overall_start = datetime.now()
    
    # Initialize components
    reader = ProductionDataReader(spark, hdfs_namenode)
    calculator = ProductionCalculator(spark, hdfs_namenode)
    writer = ProductionDataWriter(spark, hdfs_namenode)
    
    # Step 1: Load capacity data (solar panels and wind mills)
    print(f"📖 [Step 1/4] Loading capacity data...")
    step_start = datetime.now()
    reader.load_capacity_data()
    step_time = (datetime.now() - step_start).total_seconds()
    print(f"✓ Capacity data loaded in {step_time:.1f}s\n")
    
    # Step 2: Read weather data
    print(f"📖 [Step 2/4] Reading weather data from HDFS...")
    step_start = datetime.now()
    weather_data = reader.read_weather_data(last_timestamp)
    
    if weather_data.rdd.isEmpty():
        print(f"ℹ️  No new weather data to process")
        return last_timestamp
    
    record_count = weather_data.count()
    step_time = (datetime.now() - step_start).total_seconds()
    print(f"✓ Loaded {record_count:,} weather records in {step_time:.1f}s\n")
    
    # Step 3: Calculate production
    print(f"🔧 [Step 3/4] Calculating green energy production...")
    step_start = datetime.now()
    production_data = calculator.calculate_production(
        weather_data,
        reader.solar_capacity,
        reader.wind_capacity
    )
    
    production_count = production_data.count()
    step_time = (datetime.now() - step_start).total_seconds()
    print(f"✓ Calculated production for {production_count:,} records in {step_time:.1f}s\n")
    
    # Step 4: Write production data
    print(f"💾 [Step 4/4] Writing production data to HDFS...")
    step_start = datetime.now()
    max_timestamp = writer.write_production_data(production_data)
    step_time = (datetime.now() - step_start).total_seconds()
    print(f"✓ Production data written in {step_time:.1f}s\n")
    
    overall_time = (datetime.now() - overall_start).total_seconds()
    minutes = int(overall_time // 60)
    seconds = int(overall_time % 60)
    
    print(f"✅ Processing completed successfully in {minutes}m {seconds}s")
    
    return max_timestamp


def main():
    print("=" * 60)
    print("BATCH PRODUCTION JOB - Green Energy Production Calculation")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Configuration
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode-g5:9000")
    sleep_interval = int(os.getenv("SLEEP_INTERVAL_HOURS", "24")) * 3600  # Default: 24 hours
    
    print(f"Configuration:")
    print(f"  HDFS Namenode: {hdfs_namenode}")
    print(f"  Sleep Interval: {sleep_interval // 3600} hours")
    print()
    
    # Initialize Spark
    spark = init_spark_session(hdfs_namenode)
    
    try:
        while True:
            run_start = datetime.now()
            print(f"\n{'=' * 60}")
            print(f"Starting processing run at {run_start.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'=' * 60}\n")
            
            try:
                # Get last processed timestamp
                last_timestamp = get_last_processed_timestamp(spark, hdfs_namenode)
                
                # Process data
                max_timestamp = process_production_data(spark, hdfs_namenode, last_timestamp)
                
                # Save the new last processed timestamp
                if max_timestamp:
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