"""
Production calculator for green energy
Calculates wind and solar production from weather forecasts
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class ProductionCalculator:
    """Calculates green energy production from weather data"""
    
    # Solar and wind capacities by municipality (kW)
    # These are placeholder values - replace with actual capacity data
    SOLAR_CAPACITY = {
        # DK1 municipalities (>400)
        461: 50000, 851: 45000, 615: 40000, 730: 38000, 706: 35000,
        # DK2 municipalities (<=400)
        101: 55000, 147: 50000, 185: 48000, 259: 46000, 270: 44000,
    }
    
    WIND_CAPACITY = {
        # DK1 municipalities (>400)
        461: 100000, 851: 95000, 615: 90000, 730: 85000, 706: 80000,
        # DK2 municipalities (<=400)
        101: 110000, 147: 105000, 185: 100000, 259: 95000, 270: 90000,
    }
    
    def __init__(self, spark):
        self.spark = spark
    
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
            DataFrame with columns: timestamp, municipalityCode, dkArea,
                                   windProductionKwh, sunProductionKwh, productionKwh
        """
        logger.info("Calculating green energy production from forecasts...")
        
        # Aggregate sun by timestamp and municipality
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
        
        return result
    
    def _calc_solar_prod(self, muni_code, radiation):
        """Calculate solar production for single record"""
        try:
            capacity_kw = self.SOLAR_CAPACITY.get(int(muni_code), 0.0)
            if capacity_kw == 0 or pd.isnull(radiation):
                return 0.0
            
            EFFICIENCY = 0.15
            production = capacity_kw * (float(radiation) / 1000.0) * EFFICIENCY
            return float(production)
        except:
            return 0.0
    
    def _calc_wind_prod(self, muni_code, wind_speed):
        """Calculate wind production for single record"""
        try:
            capacity_kw = self.WIND_CAPACITY.get(int(muni_code), 0.0)
            if capacity_kw == 0 or pd.isnull(wind_speed):
                return 0.0
            
            cf = self._wind_capacity_factor(float(wind_speed))
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