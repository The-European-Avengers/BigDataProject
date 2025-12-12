from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, pandas_udf, lit
from pyspark.sql.types import DoubleType
import pandas as pd
import numpy as np


class ProductionCalculator:
    """Handles calculation of green energy production from weather data."""
    
    def __init__(self, spark: SparkSession, hdfs_namenode: str):
        self.spark = spark
        self.hdfs_namenode = hdfs_namenode
        self.bc_solar_capacity = None
        self.bc_wind_capacity = None
    
    def calculate_production(self, weather_df, solar_capacity, wind_capacity):
        """
        Calculate green energy production from weather data.
        
        Args:
            weather_df: DataFrame with columns: timeObserved, municipalityCode, dkArea, 
                        mean_wind_speed, mean_radiation
            solar_capacity: Dict mapping municipalityCode -> solar capacity (kW)
            wind_capacity: Dict mapping municipalityCode -> wind capacity (kW)
        
        Returns:
            DataFrame with production data
        """
        # Broadcast capacity lookups to executors
        self.bc_solar_capacity = self.spark.sparkContext.broadcast(solar_capacity)
        self.bc_wind_capacity = self.spark.sparkContext.broadcast(wind_capacity)
        
        # Step 1: Aggregate weather data by timeObserved, municipalityCode, dkArea
        # Take average of non-zero values, or 0 if all values are 0
        print("  Aggregating weather data by municipality and time...")
        
        aggregated = weather_df.groupBy("timeObserved", "municipalityCode", "dkArea").agg(
            self._avg_non_zero(col("mean_wind_speed")).alias("avg_wind_speed"),
            self._avg_non_zero(col("mean_radiation")).alias("avg_radiation")
        )
        
        # Step 2: Calculate wind production
        print("  Calculating wind energy production...")
        aggregated = aggregated.withColumn(
            "windProductionKwh",
            self._calculate_wind_production_udf(
                col("municipalityCode"),
                col("avg_wind_speed")
            )
        )
        
        # Step 3: Calculate solar production
        print("  Calculating solar energy production...")
        aggregated = aggregated.withColumn(
            "sunProductionKwh",
            self._calculate_solar_production_udf(
                col("municipalityCode"),
                col("avg_radiation")
            )
        )
        
        # Step 4: Calculate total production
        aggregated = aggregated.withColumn(
            "productionKwh",
            col("windProductionKwh") + col("sunProductionKwh")
        )
        
        # Step 5: Select final columns
        result = aggregated.select(
            col("timeObserved"),
            col("municipalityCode"),
            col("dkArea"),
            col("windProductionKwh"),
            col("sunProductionKwh"),
            col("productionKwh")
        )
        
        print("  ✓ Production calculation complete")
        
        return result
    
    def _avg_non_zero(self, column):
        """
        Calculate average of non-zero values, or 0 if all values are 0 or null.
        """
        return when(
            avg(when(column > 0, column).otherwise(None)).isNull(),
            0.0
        ).otherwise(
            avg(when(column > 0, column).otherwise(None))
        )
    
    @staticmethod
    def _wind_capacity_factor(v):
        """
        Returns the wind production capacity factor for a given wind speed (m/s).
        """
        if v < 3:
            return 0.0
        elif v < 4:
            return 0.05
        elif v < 5:
            return 0.12
        elif v < 6:
            return 0.22
        elif v < 7:
            return 0.35
        elif v < 8:
            return 0.50
        elif v < 9:
            return 0.65
        elif v < 10:
            return 0.80
        elif v < 12:
            return 0.95
        elif v < 25:
            return 0.90
        else:
            return 0.0
    
    def _calculate_wind_production_udf(self, municipality_code_col, wind_speed_col):
        """
        UDF to calculate wind production in kWh.
        
        Formula: wind_capacity (kW) * capacity_factor * 1 hour = kWh
        """
        # Capture broadcast variable in local scope to avoid serializing self
        bc_wind_cap = self.bc_wind_capacity
        
        @pandas_udf(DoubleType())
        def wind_production_udf(municipality_codes: pd.Series, wind_speeds: pd.Series) -> pd.Series:
            if bc_wind_cap is None:
                return pd.Series([0.0] * len(municipality_codes))
            
            wind_cap_dict = bc_wind_cap.value
            results = []
            
            # Vectorize the capacity factor function
            wind_cf_vec = np.vectorize(ProductionCalculator._wind_capacity_factor)
            
            for muni_code, wind_speed in zip(municipality_codes, wind_speeds):
                try:
                    if pd.isnull(muni_code) or pd.isnull(wind_speed):
                        results.append(0.0)
                        continue
                    
                    # Get wind capacity for this municipality
                    code = int(muni_code)
                    capacity_kw = wind_cap_dict.get(code, 0.0)
                    
                    if capacity_kw == 0:
                        results.append(0.0)
                        continue
                    
                    # Calculate capacity factor
                    capacity_factor = ProductionCalculator._wind_capacity_factor(float(wind_speed))
                    
                    # Calculate production (kWh for 1 hour)
                    production = capacity_kw * capacity_factor
                    results.append(float(production))
                    
                except Exception:
                    results.append(0.0)
            
            return pd.Series(results, dtype='float64')
        
        return wind_production_udf(municipality_code_col, wind_speed_col)
    
    def _calculate_solar_production_udf(self, municipality_code_col, radiation_col):
        """
        UDF to calculate solar production in kWh.
        
        Formula: pv_capacity (kW) * (mean_radiation / 1000) * efficiency * 1 hour = kWh
        
        Where:
        - mean_radiation is in W/m²
        - efficiency = 0.15 (15%)
        - 1 hour is the time period
        """
        # Capture broadcast variable in local scope to avoid serializing self
        bc_solar_cap = self.bc_solar_capacity
        
        @pandas_udf(DoubleType())
        def solar_production_udf(municipality_codes: pd.Series, radiations: pd.Series) -> pd.Series:
            if bc_solar_cap is None:
                return pd.Series([0.0] * len(municipality_codes))
            
            solar_cap_dict = bc_solar_cap.value
            results = []
            
            # Solar panel efficiency
            efficiency = 0.15
            
            for muni_code, radiation in zip(municipality_codes, radiations):
                try:
                    if pd.isnull(muni_code) or pd.isnull(radiation):
                        results.append(0.0)
                        continue
                    
                    # Get solar capacity for this municipality
                    code = int(muni_code)
                    capacity_kw = solar_cap_dict.get(code, 0.0)
                    
                    if capacity_kw == 0:
                        results.append(0.0)
                        continue
                    
                    # Calculate production
                    # radiation is in W/m², convert to kW/m² by dividing by 1000
                    # Then multiply by capacity and efficiency
                    # For 1 hour period, this gives kWh
                    radiation_value = float(radiation)
                    production = capacity_kw * (radiation_value / 1000.0) * efficiency
                    results.append(float(production))
                    
                except Exception:
                    results.append(0.0)
            
            return pd.Series(results, dtype='float64')
        
        return solar_production_udf(municipality_code_col, radiation_col)