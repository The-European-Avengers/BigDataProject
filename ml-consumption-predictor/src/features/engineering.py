"""
Feature engineering for ML model using PySpark
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging
import math

logger = logging.getLogger(__name__)


class FeatureEngineering:
    """Creates features for ML model using Spark"""
    
    @staticmethod
    def merge_consumption_weather(
        consumption_df: DataFrame,
        temp_df: DataFrame,
        sun_df: DataFrame
    ) -> DataFrame:
        """
        Merge consumption with weather data
        
        NEW consumption schema: datetime, municipalityCode, consumptionKwh, timeDK, municipality, regionName, dkArea
        
        Args:
            consumption_df: Consumption DataFrame (with timeDK, municipalityCode, consumptionKwh)
            temp_df: Temperature DataFrame (timestamp, municipalityCode, mean_temp, ...)
            sun_df: Sunlight DataFrame (timestamp, municipalityCode, mean_radiation, ...)
        
        Returns:
            Merged DataFrame with temperature, sunlight columns
        """
        logger.info("Merging consumption and weather data...")
        
        # Aggregate temperature by timestamp and municipality
        temp_agg = temp_df.groupBy("timestamp", "municipalityCode") \
            .agg(F.avg("value").alias("temperature"))
        
        # Aggregate sunlight by timestamp and municipality
        sun_agg = sun_df.groupBy("timestamp", "municipalityCode") \
            .agg(F.avg("value").alias("sunlight"))
        
        # Merge consumption with temperature
        merged = consumption_df.join(
            temp_agg,
            (consumption_df.timeDK == temp_agg.timestamp) &
            (consumption_df.municipalityCode == temp_agg.municipalityCode),
            "left"
        ).drop(temp_agg.timestamp).drop(temp_agg.municipalityCode)
        
        # Merge with sunlight
        merged = merged.join(
            sun_agg,
            (merged.timeDK == sun_agg.timestamp) &
            (merged.municipalityCode == sun_agg.municipalityCode),
            "left"
        ).drop(sun_agg.timestamp).drop(sun_agg.municipalityCode)
        
        # Fill missing weather values with reasonable defaults
        merged = merged.fillna({
            'temperature': 10.0,
            'sunlight': 0.0
        })
        
        logger.info("Data merged successfully")
        return merged
    
    @staticmethod
    def create_time_features(df: DataFrame, time_col: str = "timeDK") -> DataFrame:
        """
        Create time-based features
        
        Args:
            df: Input DataFrame
            time_col: Name of timestamp column (timeDK for consumption, timestamp for forecast)
        
        Returns:
            DataFrame with time features added
        """
        logger.info("Creating time features...")
        
        df = df.withColumn("hour", F.hour(time_col))
        df = df.withColumn("day_of_week", F.dayofweek(time_col) - 1)
        df = df.withColumn("month", F.month(time_col))
        df = df.withColumn("day_of_year", F.dayofyear(time_col))
        df = df.withColumn("is_weekend", 
                          F.when(F.col("day_of_week") >= 5, 1).otherwise(0))
        
        # Cyclical encoding for hour
        df = df.withColumn("hour_sin", 
                          F.sin(F.lit(2 * math.pi) * F.col("hour") / 24))
        df = df.withColumn("hour_cos", 
                          F.cos(F.lit(2 * math.pi) * F.col("hour") / 24))
        
        # Cyclical encoding for month
        df = df.withColumn("month_sin", 
                          F.sin(F.lit(2 * math.pi) * F.col("month") / 12))
        df = df.withColumn("month_cos", 
                          F.cos(F.lit(2 * math.pi) * F.col("month") / 12))
        
        return df
    
    @staticmethod
    def create_interaction_features(df: DataFrame) -> DataFrame:
        """
        Create interaction features
        
        Args:
            df: Input DataFrame (must have temperature, sunlight columns)
        
        Returns:
            DataFrame with interaction features added
        """
        logger.info("Creating interaction features...")
        
        df = df.withColumn("temp_x_sunlight", 
                          F.col("temperature") * F.col("sunlight"))
        df = df.withColumn("temp_squared", 
                          F.col("temperature") * F.col("temperature"))
        df = df.withColumn("is_cold", 
                          F.when(F.col("temperature") < 5, 1).otherwise(0))
        df = df.withColumn("is_dark", 
                          F.when(F.col("sunlight") < 100, 1).otherwise(0))
        df = df.withColumn("cold_and_dark", 
                          F.col("is_cold") * F.col("is_dark"))
        
        return df
    
    @staticmethod
    def create_lag_features(df: DataFrame) -> DataFrame:
        """
        Create seasonal lag features from previous year
        
        NEW: Uses consumptionKwh column (capital K)
        
        Args:
            df: Input DataFrame with consumption data (must have consumptionKwh column)
        
        Returns:
            DataFrame with lag features added
        """
        logger.info("Creating lag features...")
        
        # Define window for lag operations
        window_spec = Window.partitionBy("municipalityCode").orderBy("timeDK")
        
        # Lag features (8760 hours = 365 days)
        hours_in_year = 8760
        
        # Same hour last year
        df = df.withColumn(
            "consumption_same_hour_last_year",
            F.lag("consumptionKwh", hours_in_year).over(window_spec)
        )
        
        # Rolling average for same day last year (24 hours window)
        window_rolling = Window.partitionBy("municipalityCode") \
            .orderBy("timeDK") \
            .rowsBetween(-hours_in_year - 12, -hours_in_year + 12)
        
        df = df.withColumn(
            "consumption_same_day_last_year",
            F.avg("consumptionKwh").over(window_rolling)
        )
        
        # Fill nulls with municipality average
        window_muni_avg = Window.partitionBy("municipalityCode")
        muni_avg = F.avg("consumptionKwh").over(window_muni_avg)
        
        df = df.withColumn(
            "consumption_same_hour_last_year",
            F.coalesce(F.col("consumption_same_hour_last_year"), muni_avg)
        )
        df = df.withColumn(
            "consumption_same_day_last_year",
            F.coalesce(F.col("consumption_same_day_last_year"), muni_avg)
        )
        
        return df
    
    @staticmethod
    def prepare_forecast_features(
        forecast_df: DataFrame,
        historical_df: DataFrame
    ) -> DataFrame:
        """
        Prepare features for forecast data
        
        Args:
            forecast_df: Forecast DataFrame with weather (timestamp, municipalityCode, temperature, sunlight)
            historical_df: Historical DataFrame for lag features
        
        Returns:
            Forecast DataFrame with all features
        """
        logger.info("Preparing forecast features...")
        
        # Add time features
        forecast_df = FeatureEngineering.create_time_features(forecast_df, "timestamp")
        
        # Add interaction features
        forecast_df = FeatureEngineering.create_interaction_features(forecast_df)
        
        # Calculate historical averages for lag features (use consumptionKwh with capital K)
        historical_avg = historical_df.groupBy("municipalityCode") \
            .agg(F.avg("consumptionKwh").alias("avg_consumption"))
        
        # Join with forecast
        forecast_df = forecast_df.join(
            historical_avg,
            forecast_df.municipalityCode == historical_avg.municipalityCode,
            "left"
        ).drop(historical_avg.municipalityCode)
        
        # Use historical average as proxy for lag features
        forecast_df = forecast_df.withColumn(
            "consumption_same_hour_last_year",
            F.coalesce(F.col("avg_consumption"), F.lit(1500.0))
        )
        forecast_df = forecast_df.withColumn(
            "consumption_same_day_last_year",
            F.coalesce(F.col("avg_consumption"), F.lit(1500.0))
        )
        
        # Rename timestamp to timeDK for consistency (if needed for downstream)
        if "timestamp" in forecast_df.columns and "timeDK" not in forecast_df.columns:
            forecast_df = forecast_df.withColumnRenamed("timestamp", "timeDK")
        
        # Drop temporary column
        forecast_df = forecast_df.drop("avg_consumption")
        
        return forecast_df