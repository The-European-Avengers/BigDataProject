from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
import pandas as pd


class ProductionDataReader:
    """Handles reading weather data and capacity data from HDFS."""
    
    def __init__(self, spark: SparkSession, hdfs_namenode: str):
        self.spark = spark
        self.hdfs_namenode = hdfs_namenode
        self.solar_capacity = None
        self.wind_capacity = None
    
    def load_capacity_data(self):
        """
        Load solar panel and wind mill capacity data from HDFS.
        Creates lookup dictionaries: municipalityCode -> capacity (kW)
        """
        # Load solar panels data
        solar_path = f"{self.hdfs_namenode}/utils/solar_panels.csv"
        print(f"  Loading solar panels from: {solar_path}")
        
        try:
            solar_df = self.spark.read.csv(solar_path, header=True, inferSchema=True)
            solar_pd = solar_df.toPandas()
            
            # Drop unnecessary columns and sort
            solar_pd = solar_pd.drop(columns=['kw_smaa', 'kw_mellem', 'kw_store', 
                                              'anl_total', 'anl_smaa', 'anl_mellem', 'anl_store'])
            solar_pd.sort_values(by=['kommune'], inplace=True)
            
            # Create lookup: municipalityCode (komnr) -> total capacity (kw_total)
            self.solar_capacity = dict(zip(solar_pd['komnr'], solar_pd['kw_total']))
            
            print(f"    ✓ Loaded {len(self.solar_capacity)} solar capacity entries")
            
        except Exception as e:
            print(f"    ⚠️  Warning: Could not load solar panels data: {e}")
            self.solar_capacity = {}
        
        # Load wind mills data
        wind_path = f"{self.hdfs_namenode}/utils/wind_mills.csv"
        print(f"  Loading wind mills from: {wind_path}")
        
        try:
            wind_df = self.spark.read.csv(wind_path, header=True, inferSchema=True)
            wind_pd = wind_df.toPandas()
            
            # Drop unnecessary columns
            wind_pd = wind_pd.drop(columns=['Shortest distance', 'Average distance', 
                                            'Longest distance', 'Inhabitants'])
            
            # Load municipality codes to map city names to codes
            municipality_path = f"{self.hdfs_namenode}/utils/municipality_codes_to_coordinates.csv"
            muni_df = self.spark.read.csv(municipality_path, header=True, inferSchema=True)
            muni_pd = muni_df.toPandas()
            
            # Create mapping: city name -> code
            city_to_code = dict(zip(muni_pd['name'], muni_pd['code']))
            
            # Map Kommune (city name) to code
            wind_pd['code'] = wind_pd['Kommune'].map(city_to_code)
            wind_pd = wind_pd.dropna(subset=['code'])
            wind_pd['code'] = wind_pd['code'].astype(int)
            
            # Create lookup: municipalityCode -> total installed capacity
            # Group by code in case multiple entries exist
            wind_grouped = wind_pd.groupby('code')['Installed capacity [kW]'].sum()
            self.wind_capacity = wind_grouped.to_dict()
            
            print(f"    ✓ Loaded {len(self.wind_capacity)} wind capacity entries")
            
        except Exception as e:
            print(f"    ⚠️  Warning: Could not load wind mills data: {e}")
            self.wind_capacity = {}
    
    def read_weather_data(self, last_timestamp=None):
        """
        Read weather data from HDFS for all available years and months.
        
        Data is stored in:
          /historical/{year}/weather-wind/{month}.avro/part-*.avro
          /historical/{year}/weather-sun/{month}.avro/part-*.avro
        
        Schema for wind: timeObserved, stationId, stationName, mean_wind_speed, 
                         lon, lat, dkArea, municipalityCode
        Schema for sun:  timeObserved, stationId, stationName, mean_radiation, 
                         lon, lat, dkArea, municipalityCode
        
        Args:
            last_timestamp: Only process data after this timestamp (None for first run)
        
        Returns:
            Unified DataFrame with weather data
        """
        # Discover available year/month combinations
        year_months = self._discover_weather_files()
        
        if not year_months:
            print("  ⚠️  No weather data files found")
            return self.spark.createDataFrame([], schema="timeObserved timestamp, municipalityCode int")
        
        print(f"  Found {len(year_months)} year-month combinations to process")
        
        all_wind_data = []
        all_sun_data = []
        
        for year_val, month_val in year_months:
            # Read wind data
            wind_path = f"{self.hdfs_namenode}/historical/{year_val}/weather-wind/{month_val:02d}.avro"
            try:
                wind_df = self.spark.read.format("avro").load(wind_path)
                
                # Filter by timestamp if needed
                if last_timestamp:
                    wind_df = wind_df.filter(col("timeObserved") > last_timestamp)
                
                if not wind_df.rdd.isEmpty():
                    # Select and rename columns
                    wind_df = wind_df.select(
                        col("timeObserved"),
                        col("municipalityCode"),
                        col("dkArea"),
                        col("mean_wind_speed")
                    )
                    all_wind_data.append(wind_df)
                    
            except Exception as e:
                print(f"    ⚠️  Could not read wind data for {year_val}-{month_val:02d}: {e}")
            
            # Read sun data
            sun_path = f"{self.hdfs_namenode}/historical/{year_val}/weather-sun/{month_val:02d}.avro"
            try:
                sun_df = self.spark.read.format("avro").load(sun_path)
                
                # Filter by timestamp if needed
                if last_timestamp:
                    sun_df = sun_df.filter(col("timeObserved") > last_timestamp)
                
                if not sun_df.rdd.isEmpty():
                    # Select and rename columns
                    sun_df = sun_df.select(
                        col("timeObserved"),
                        col("municipalityCode"),
                        col("dkArea"),
                        col("mean_radiation")
                    )
                    all_sun_data.append(sun_df)
                    
            except Exception as e:
                print(f"    ⚠️  Could not read sun data for {year_val}-{month_val:02d}: {e}")
        
        # Union all wind and sun data
        if all_wind_data:
            wind_combined = all_wind_data[0]
            for df in all_wind_data[1:]:
                wind_combined = wind_combined.union(df)
        else:
            wind_combined = None
        
        if all_sun_data:
            sun_combined = all_sun_data[0]
            for df in all_sun_data[1:]:
                sun_combined = sun_combined.union(df)
        else:
            sun_combined = None
        
        # Join wind and sun data on timeObserved, municipalityCode, dkArea
        if wind_combined and sun_combined:
            weather_data = wind_combined.join(
                sun_combined,
                on=["timeObserved", "municipalityCode", "dkArea"],
                how="full_outer"
            )
        elif wind_combined:
            weather_data = wind_combined.withColumn("mean_radiation", col("mean_wind_speed") * 0)  # Add null column
        elif sun_combined:
            weather_data = sun_combined.withColumn("mean_wind_speed", col("mean_radiation") * 0)  # Add null column
        else:
            # Return empty DataFrame with correct schema
            return self.spark.createDataFrame(
                [], 
                schema="timeObserved timestamp, municipalityCode int, dkArea int, mean_wind_speed double, mean_radiation double"
            )
        
        print(f"  ✓ Weather data combined successfully")
        
        return weather_data
    
    def _discover_weather_files(self):
        """
        Discover all available year/month combinations in HDFS.
        
        Returns:
            List of (year, month) tuples
        """
        year_months = set()
        
        try:
            # List all years in /historical/
            sc = self.spark.sparkContext
            hadoop_conf = sc._jsc.hadoopConfiguration()
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                sc._jvm.java.net.URI(self.hdfs_namenode),
                hadoop_conf
            )
            
            historical_path = sc._jvm.org.apache.hadoop.fs.Path(f"{self.hdfs_namenode}/historical")
            
            if not fs.exists(historical_path):
                return []
            
            # List all year directories
            year_status = fs.listStatus(historical_path)
            
            for status in year_status:
                year_path_str = status.getPath().toString()
                year_name = year_path_str.split("/")[-1]
                
                # Check if it's a valid year (numeric)
                try:
                    year_val = int(year_name)
                except ValueError:
                    continue
                
                # Check for weather-wind and weather-sun directories
                for weather_type in ["weather-wind", "weather-sun"]:
                    weather_path = sc._jvm.org.apache.hadoop.fs.Path(
                        f"{self.hdfs_namenode}/historical/{year_val}/{weather_type}"
                    )
                    
                    if fs.exists(weather_path):
                        # List all month files
                        month_status = fs.listStatus(weather_path)
                        
                        for month_stat in month_status:
                            month_path_str = month_stat.getPath().toString()
                            month_file = month_path_str.split("/")[-1]
                            
                            # Extract month from filename (e.g., "01.avro" -> 1)
                            if month_file.endswith(".avro"):
                                try:
                                    month_val = int(month_file.split(".")[0])
                                    if 1 <= month_val <= 12:
                                        year_months.add((year_val, month_val))
                                except ValueError:
                                    continue
            
        except Exception as e:
            print(f"  ⚠️  Error discovering weather files: {e}")
            return []
        
        # Return sorted list
        return sorted(list(year_months))