"""
Production calculator for green energy
Calculates wind and solar production from weather forecasts
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import pandas as pd
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


class ProductionCalculator:
    """Calculates green energy production from weather data"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.solar_capacity = {}
        self.wind_capacity = {}
        self._load_capacity_data()
    
    def _load_capacity_data(self):
        """
        Load solar panel and wind mill capacity data from CSV files.
        
        For local mode: Looks in ml-consumption-predictor/utils/
        For k8s mode: Looks in HDFS at /utils/
        """
        from src.config.settings import settings
        
        try:
            if settings.is_local:
                # Local mode: Load from project utils folder
                project_root = settings.paths.project_root
                utils_path = project_root / "utils"
                
                solar_path = str(utils_path / "solar_panels.csv")
                wind_path = str(utils_path / "wind_mills.csv")
                municipality_path = str(utils_path / "municipality_codes_to_coordinates.csv")
                
                logger.info(f"Loading capacity data from local files:")
                logger.info(f"  Solar: {solar_path}")
                logger.info(f"  Wind: {wind_path}")
                logger.info(f"  Municipalities: {municipality_path}")
                
            else:
                # Kubernetes mode: Load from HDFS
                hdfs_base = settings.paths.base_path
                solar_path = f"{hdfs_base}/utils/solar_panels.csv"
                wind_path = f"{hdfs_base}/utils/wind_mills.csv"
                municipality_path = f"{hdfs_base}/utils/municipality_codes_to_coordinates.csv"
                
                logger.info(f"Loading capacity data from HDFS:")
                logger.info(f"  Solar: {solar_path}")
                logger.info(f"  Wind: {wind_path}")
                logger.info(f"  Municipalities: {municipality_path}")
            
            # Load solar panels
            self._load_solar_capacity(solar_path)
            
            # Load wind mills (needs municipality mapping)
            self._load_wind_capacity(wind_path, municipality_path)
            
            logger.info(f"✓ Capacity data loaded successfully")
            logger.info(f"  Solar: {len(self.solar_capacity)} municipalities")
            logger.info(f"  Wind: {len(self.wind_capacity)} municipalities")
            
        except Exception as e:
            logger.error(f"Failed to load capacity data: {e}")
            logger.warning("Using empty capacity dictionaries - production will be 0.0")
            self.solar_capacity = {}
            self.wind_capacity = {}
    
    def _load_solar_capacity(self, solar_path: str):
        """
        Load solar panel capacity data.
        
        CSV columns: komnr, kommune, kw_smaa, kw_mellem, kw_store, kw_total, 
                     anl_total, anl_smaa, anl_mellem, anl_store
        
        We need: komnr (municipality code) -> kw_total (total capacity in kW)
        """
        try:
            solar_df = self.spark.read.csv(solar_path, header=True, inferSchema=True)
            solar_pd = solar_df.select("komnr", "kw_total").toPandas()
            
            # Create lookup: municipalityCode -> capacity (kW)
            self.solar_capacity = dict(zip(solar_pd['komnr'], solar_pd['kw_total']))
            
            logger.info(f"  Loaded {len(self.solar_capacity)} solar capacity entries")
            
        except Exception as e:
            logger.error(f"  Failed to load solar capacity: {e}")
            self.solar_capacity = {}
    
    def _load_wind_capacity(self, wind_path: str, municipality_path: str):
        """
        Load wind mill capacity data.
        
        Wind CSV columns: Kommune, Number of mills, Installed capacity [kW], 
                         Shortest distance, Average distance, Longest distance, Inhabitants
        
        Municipality CSV columns: code, name, lat, lon
        
        We need to map Kommune (city name) -> code -> Installed capacity [kW]
        """
        try:
            # Load municipality codes to map city names to codes
            muni_df = self.spark.read.csv(municipality_path, header=True, inferSchema=True)
            muni_pd = muni_df.select("code", "name").toPandas()
            
            # Create mapping: city name -> code
            city_to_code = dict(zip(muni_pd['name'], muni_pd['code']))
            
            # Load wind mills
            wind_df = self.spark.read.csv(wind_path, header=True, inferSchema=True)
            wind_pd = wind_df.select("Kommune", "Installed capacity [kW]").toPandas()
            
            # Map Kommune (city name) to code
            wind_pd['code'] = wind_pd['Kommune'].map(city_to_code)
            wind_pd = wind_pd.dropna(subset=['code'])
            wind_pd['code'] = wind_pd['code'].astype(int)
            
            # Create lookup: municipalityCode -> total installed capacity
            # Group by code in case multiple entries exist (sum capacities)
            wind_grouped = wind_pd.groupby('code')['Installed capacity [kW]'].sum()
            self.wind_capacity = wind_grouped.to_dict()
            
            logger.info(f"  Loaded {len(self.wind_capacity)} wind capacity entries")
            
        except Exception as e:
            logger.error(f"  Failed to load wind capacity: {e}")
            self.wind_capacity = {}
    
    def calculate_production(
        self,
        temp_forecast: DataFrame,
        sun_forecast: DataFrame,
        wind_forecast: DataFrame = None
    ) -> DataFrame:
        """
        Calculate production from weather forecasts
        
        Args:
            temp_forecast: Temperature forecast (not used for production, but for merging)
            sun_forecast: Solar radiation forecast (W/m²)
            wind_forecast: Wind speed forecast (m/s)
        
        Returns:
            DataFrame with columns: timeObserved, municipalityCode, dkArea,
                                   windProductionKwh, sunProductionKwh, productionKwh
        """
        logger.info("Calculating green energy production from forecasts...")
        
        # Check if capacity data is loaded
        if not self.solar_capacity and not self.wind_capacity:
            logger.warning("No capacity data loaded! Production will be 0.0")
        
        # FIX: First floor timestamps to seconds to remove any milliseconds
        sun_forecast = sun_forecast.withColumn("timestamp", F.date_trunc("second", F.col("timestamp")))
        if wind_forecast is not None:
            wind_forecast = wind_forecast.withColumn("timestamp", F.date_trunc("second", F.col("timestamp")))
        
        # Aggregate sun by timestamp and municipality (take average if multiple readings)
        sun_agg = sun_forecast.groupBy("timestamp", "municipalityCode", "dkArea") \
            .agg(F.avg("value").alias("mean_radiation"))
        
        # Aggregate wind if available
        if wind_forecast is not None:
            wind_agg = wind_forecast.groupBy("timestamp", "municipalityCode", "dkArea") \
                .agg(F.avg("value").alias("mean_wind_speed"))
            
            # Merge sun and wind
            weather = sun_agg.join(
                wind_agg,
                on=["timestamp", "municipalityCode", "dkArea"],
                how="outer"
            )
        else:
            weather = sun_agg.withColumn("mean_wind_speed", F.lit(0.0))
        
        # Fill nulls
        weather = weather.fillna({
            'mean_radiation': 0.0,
            'mean_wind_speed': 0.0
        })
        
        # Infer dkArea if missing (municipalityCode > 400 -> DK1, else DK2)
        weather = weather.withColumn(
            "dkArea",
            F.when(F.col("dkArea").isNull(),
                  F.when(F.col("municipalityCode") > 400, 1).otherwise(2)
            ).otherwise(F.col("dkArea"))
        )
        
        # Calculate production using Python (collect to driver, calculate, broadcast back)
        # This avoids Pandas UDF which requires Arrow
        weather_pd = weather.toPandas()
        
        # CRITICAL: Ensure timestamp has no microseconds (floor to seconds)
        weather_pd['timestamp'] = pd.to_datetime(weather_pd['timestamp']).dt.floor('s')
        
        # Calculate solar production
        weather_pd['sunProductionKwh'] = weather_pd.apply(
            lambda row: self._calc_solar_prod(row['municipalityCode'], row['mean_radiation']),
            axis=1
        )
        
        # Calculate wind production
        weather_pd['windProductionKwh'] = weather_pd.apply(
            lambda row: self._calc_wind_prod(row['municipalityCode'], row['mean_wind_speed']),
            axis=1
        )
        
        # Total production
        weather_pd['productionKwh'] = weather_pd['windProductionKwh'] + weather_pd['sunProductionKwh']
        
        # Convert back to Spark DataFrame
        result = self.spark.createDataFrame(weather_pd)
        
        # Rename timestamp to timeObserved for consistency
        result = result.withColumnRenamed("timestamp", "timeObserved")
        
        logger.info(f"Calculated production for {len(weather_pd):,} records")
        
        # Log summary statistics
        total_wind = weather_pd['windProductionKwh'].sum()
        total_solar = weather_pd['sunProductionKwh'].sum()
        total_prod = weather_pd['productionKwh'].sum()
        
        logger.info(f"  Total wind production: {total_wind:,.0f} kWh")
        logger.info(f"  Total solar production: {total_solar:,.0f} kWh")
        logger.info(f"  Total production: {total_prod:,.0f} kWh")
        
        return result
    
    def _calc_solar_prod(self, muni_code, radiation):
        """Calculate solar production for single record"""
        try:
            capacity_kw = self.solar_capacity.get(int(muni_code), 0.0)
            if capacity_kw == 0 or pd.isnull(radiation):
                return 0.0
            
            # Solar panel efficiency (15%)
            EFFICIENCY = 0.15
            
            # Production = capacity * (radiation / 1000) * efficiency
            # radiation is in W/m², convert to kW/m² by dividing by 1000
            production = capacity_kw * (float(radiation) / 1000.0) * EFFICIENCY
            return float(production)
        except:
            return 0.0
    
    def _calc_wind_prod(self, muni_code, wind_speed):
        """Calculate wind production for single record"""
        try:
            capacity_kw = self.wind_capacity.get(int(muni_code), 0.0)
            if capacity_kw == 0 or pd.isnull(wind_speed):
                return 0.0
            
            # Calculate capacity factor based on wind speed
            cf = self._wind_capacity_factor(float(wind_speed))
            
            # Production = capacity * capacity_factor
            production = capacity_kw * cf
            return float(production)
        except:
            return 0.0
    
    @staticmethod
    def _wind_capacity_factor(v: float) -> float:
        """Calculate wind production capacity factor from wind speed (m/s)"""
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