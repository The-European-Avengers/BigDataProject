from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys


class PrecisionDataReader:
    """Handles reading prediction and real data from HDFS."""
    
    def __init__(self, spark: SparkSession, hdfs_namenode: str):
        self.spark = spark
        self.hdfs_namenode = hdfs_namenode
    
    def read_prediction_files(self, last_timestamp=None):
        """
        Read prediction parquet files from HDFS archives.
        
        Data is stored in:
          /historical/archives/{year}/{month}/analytics/predictions_*.parquet
        
        Args:
            last_timestamp: Only process files after this timestamp (None for first run)
        
        Returns:
            DataFrame with prediction data
        """
        # Discover available year/month combinations
        year_months = self._discover_archive_files()
        
        if not year_months:
            print("  ⚠️  No prediction files found")
            return self.spark.createDataFrame(
                [], 
                schema="timestamp timestamp, municipalityCode int, dkArea int, " +
                       "consumptionkWh double, mean_temp double, mean_radiation double, " +
                       "mean_wind_speed double, productionKwh double, price double, " +
                       "realConsumptionKwh double, realPrice_EUR_MWh double"
            )
        
        print(f"  Found {len(year_months)} year-month combinations to process")
        
        all_predictions = []
        
        for year_val, month_val in year_months:
            # List all prediction parquet files in the analytics directory
            analytics_dir = f"{self.hdfs_namenode}/historical/archives/{year_val}/{month_val:02d}/analytics"
            
            try:
                # Use Hadoop FileSystem API to list files
                sc = self.spark.sparkContext
                hadoop_conf = sc._jsc.hadoopConfiguration()
                fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                    sc._jvm.java.net.URI(self.hdfs_namenode),
                    hadoop_conf
                )
                
                analytics_path = sc._jvm.org.apache.hadoop.fs.Path(analytics_dir)
                
                if not fs.exists(analytics_path):
                    print(f"    ⚠️  Analytics directory does not exist for {year_val}-{month_val:02d}")
                    continue
                
                # List all files in analytics directory
                file_status = fs.listStatus(analytics_path)
                prediction_files = []
                
                for status in file_status:
                    file_name = status.getPath().getName()
                    # Only include files that start with "predictions_"
                    if file_name.startswith("predictions_") and file_name.endswith(".parquet"):
                        full_path = status.getPath().toString()
                        prediction_files.append(full_path)
                
                if not prediction_files:
                    print(f"    ⚠️  No prediction files found for {year_val}-{month_val:02d}")
                    continue
                
                print(f"    Found {len(prediction_files)} prediction file(s) for {year_val}-{month_val:02d}")
                
                # Read all prediction files
                pred_df = self.spark.read.parquet(*prediction_files)
                
                # Filter by timestamp if needed
                if last_timestamp:
                    pred_df = pred_df.filter(col("timestamp") > last_timestamp)
                
                if not pred_df.rdd.isEmpty():
                    all_predictions.append(pred_df)
                    
            except Exception as e:
                print(f"    ⚠️  Could not read predictions for {year_val}-{month_val:02d}: {e}")
        
        # Union all prediction data
        if all_predictions:
            predictions_combined = all_predictions[0]
            for df in all_predictions[1:]:
                predictions_combined = predictions_combined.union(df)
            
            print(f"  ✓ Prediction data combined successfully")
            return predictions_combined
        else:
            return self.spark.createDataFrame(
                [], 
                schema="timestamp timestamp, municipalityCode int, dkArea int, " +
                       "consumptionkWh double, mean_temp double, mean_radiation double, " +
                       "mean_wind_speed double, productionKwh double, price double, " +
                       "realConsumptionKwh double, realPrice_EUR_MWh double"
            )
    
    def read_real_consumption(self, year_val, month_val):
        """
        Read real consumption data for a specific year and month.
        
        Data is stored in:
          /historical/{year}/consumption/{month}.avro
        
        Returns:
            DataFrame with columns: datetime (renamed to timestamp), municipalityCode, consumptionKwh
        """
        consumption_path = f"{self.hdfs_namenode}/historical/{year_val}/consumption/{month_val:02d}.avro"
        
        try:
            cons_df = self.spark.read.format("avro").load(consumption_path)
            
            # Rename datetime to timestamp and consumptionKwh to realConsumptionKwh
            cons_df = cons_df.select(
                col("datetime").alias("timestamp"),
                col("municipalityCode"),
                col("consumptionKwh").alias("realConsumptionKwh")
            )
            
            return cons_df
            
        except Exception as e:
            print(f"    ⚠️  Could not read consumption data for {year_val}-{month_val:02d}: {e}")
            return None
    
    def read_real_price(self, year_val):
        """
        Read real price data for a specific year.
        
        Data is stored in:
          /historical/{year}/price.avro
        
        Returns:
            DataFrame with columns: timestamp, dkArea, price_EUR_MWh
        """
        price_path = f"{self.hdfs_namenode}/historical/{year_val}/price.avro"
        
        try:
            price_df = self.spark.read.format("avro").load(price_path)
            
            # Select relevant columns and rename
            price_df = price_df.select(
                col("timestamp"),
                col("dkArea"),
                col("price_EUR_MWh").alias("realPrice_EUR_MWh")
            )
            
            return price_df
            
        except Exception as e:
            print(f"    ⚠️  Could not read price data for {year_val}: {e}")
            return None
    
    def _discover_archive_files(self):
        """
        Discover all available year/month combinations in HDFS archives.
        
        Returns:
            List of (year, month) tuples
        """
        year_months = set()
        
        print("  🔍 Scanning HDFS archives directory...")
        sys.stdout.flush()
        
        try:
            sc = self.spark.sparkContext
            hadoop_conf = sc._jsc.hadoopConfiguration()
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                sc._jvm.java.net.URI(self.hdfs_namenode),
                hadoop_conf
            )
            
            archives_path = sc._jvm.org.apache.hadoop.fs.Path(
                f"{self.hdfs_namenode}/historical/archives"
            )
            
            if not fs.exists(archives_path):
                print("  ⚠️  Archives path does not exist")
                sys.stdout.flush()
                return []
            
            print("  📂 Listing year directories...")
            sys.stdout.flush()
            
            # List all year directories
            year_status = fs.listStatus(archives_path)
            print(f"  Found {len(year_status)} entries in archives")
            sys.stdout.flush()
            
            for year_stat in year_status:
                year_path_str = year_stat.getPath().toString()
                year_name = year_path_str.split("/")[-1]
                
                print(f"    Checking year: {year_name}")
                sys.stdout.flush()
                
                # Check if it's a valid year (numeric)
                try:
                    year_val = int(year_name)
                except ValueError:
                    print(f"    ⏭️  Skipping non-numeric: {year_name}")
                    sys.stdout.flush()
                    continue
                
                # List all month directories
                year_path = sc._jvm.org.apache.hadoop.fs.Path(
                    f"{self.hdfs_namenode}/historical/archives/{year_val}"
                )
                
                if fs.exists(year_path):
                    month_status = fs.listStatus(year_path)
                    
                    for month_stat in month_status:
                        month_path_str = month_stat.getPath().toString()
                        month_name = month_path_str.split("/")[-1]
                        
                        # Check if it's a valid month (numeric)
                        try:
                            month_val = int(month_name)
                            if 1 <= month_val <= 12:
                                # Check if analytics folder exists
                                analytics_path = sc._jvm.org.apache.hadoop.fs.Path(
                                    f"{self.hdfs_namenode}/historical/archives/{year_val}/{month_val:02d}/analytics"
                                )
                                
                                if fs.exists(analytics_path):
                                    year_months.add((year_val, month_val))
                        except ValueError:
                            continue
            
        except Exception as e:
            print(f"  ⚠️  Error discovering archive files: {e}")
            return []
        
        # Return sorted list
        return sorted(list(year_months))