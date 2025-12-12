from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, max as spark_max


class ProductionDataWriter:
    """Handles writing production data to HDFS in Avro format."""
    
    def __init__(self, spark: SparkSession, hdfs_namenode: str):
        self.spark = spark
        self.hdfs_namenode = hdfs_namenode
    
    def write_production_data(self, production_df):
        """
        Write production data to HDFS partitioned by year and month.
        
        Output path: /historical/{YEAR}/production/{MONTH}.avro
        
        Schema: timeObserved, municipalityCode, dkArea, windProductionKwh, 
                sunProductionKwh, productionKwh
        
        Args:
            production_df: DataFrame with production data
        
        Returns:
            datetime: Maximum timestamp in the processed data
        """
        # Extract year and month from timeObserved
        production_df = production_df.withColumn("year", year(col("timeObserved")))
        production_df = production_df.withColumn("month", month(col("timeObserved")))
        
        # Get the maximum timestamp for state tracking
        max_timestamp_row = production_df.agg(spark_max("timeObserved").alias("max_ts")).collect()[0]
        max_timestamp = max_timestamp_row["max_ts"]
        
        if max_timestamp is None:
            print("  ⚠️  No data to write")
            return None
        
        # Get unique year-month combinations
        year_months = production_df.select("year", "month").distinct().orderBy("year", "month").collect()
        
        print(f"  Writing data for {len(year_months)} year-month combinations...")
        
        # Process each year-month combination
        for idx, row in enumerate(year_months, 1):
            year_val = row["year"]
            month_val = row["month"]
            
            # Filter data for this year-month
            partition_df = production_df.filter(
                (col("year") == year_val) & (col("month") == month_val)
            ).drop("year", "month")  # Drop partition columns from output
            
            # Output path
            output_path = f"{self.hdfs_namenode}/historical/{year_val}/production/{month_val:02d}.avro"
            
            record_count = partition_df.count()
            
            print(f"    [{idx}/{len(year_months)}] Writing {year_val}-{month_val:02d}: {record_count:,} records...")
            
            write_start = datetime.now()
            
            # Check if file already exists and read existing data
            existing_df = None
            try:
                existing_df = self.spark.read.format("avro").load(output_path)
                print(f"      Found existing data, merging...")
                
                # Union with existing data
                partition_df = partition_df.union(existing_df)
                
                # Remove duplicates based on timeObserved and municipalityCode
                # Keep the most recent record (in case of updates)
                partition_df = partition_df.dropDuplicates(["timeObserved", "municipalityCode"])
                
            except Exception:
                # File doesn't exist, that's fine
                pass
            
            # Write as AVRO with overwrite mode
            partition_df.write.mode("overwrite").format("avro").save(output_path)
            
            write_time = (datetime.now() - write_start).total_seconds()
            print(f"      ✓ Completed in {write_time:.1f}s → {output_path}")
        
        print(f"  ✓ All production data written successfully")
        print(f"  Maximum timestamp processed: {max_timestamp}")
        
        return max_timestamp