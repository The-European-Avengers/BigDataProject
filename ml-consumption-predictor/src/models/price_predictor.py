"""
Price prediction logic
Predicts prices for DK1 and DK2 based on consumption and production forecasts
"""

import xgboost as xgb
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pandas as pd
import logging
import math
from typing import Dict

from src.config.settings import settings
from src.utils.spark_utils import get_spark

logger = logging.getLogger(__name__)


class PricePredictor:
    """Predicts electricity prices for DK areas"""
    
    def __init__(self, models: Dict[int, xgb.XGBRegressor]):
        """
        Initialize predictor
        
        Args:
            models: Dictionary of dkArea -> trained XGBoost model
        """
        self.models = models
        self.spark = get_spark()
        self.feature_columns = settings.model.price_feature_columns
    
    def predict(
        self,
        consumption_forecast: DataFrame,
        production_forecast: DataFrame
    ) -> DataFrame:
        """
        Generate price predictions for DK1 and DK2
        
        Args:
            consumption_forecast: Consumption predictions (timestamp, municipalityCode, 
                                                          dkArea, consumptionkWh)
            production_forecast: Production calculations (timeObserved, municipalityCode, 
                                                         dkArea, windProductionKwh, 
                                                         sunProductionKwh, productionKwh)
        
        Returns:
            DataFrame with columns: timestamp, dkArea, price
        """
        logger.info("=" * 80)
        logger.info("PRICE PREDICTION - DK1 and DK2")
        logger.info("=" * 80)
        
        # Prepare forecast features
        forecast_df = self._prepare_forecast_features(
            consumption_forecast,
            production_forecast
        )
        
        # Predict for DK1 and DK2
        predictions = []
        
        for dk_area in [1, 2]:
            logger.info(f"Predicting prices for DK{dk_area}...")
            area_predictions = self._predict_single_area(forecast_df, dk_area)
            predictions.append(area_predictions)
        
        # Combine predictions
        result_pd = pd.concat(predictions, ignore_index=True)
        result_df = self.spark.createDataFrame(result_pd)
        
        logger.info(f"Generated {result_df.count():,} price predictions")
        
        return result_df
    
    def _prepare_forecast_features(
        self,
        consumption_forecast: DataFrame,
        production_forecast: DataFrame
    ) -> DataFrame:
        """
        Prepare aggregated forecast features for price prediction
        
        Returns:
            DataFrame with features ready for prediction
        """
        logger.info("Preparing price forecast features...")
        
        # Ensure dkArea exists
        if "dkArea" not in consumption_forecast.columns:
            consumption_forecast = consumption_forecast.withColumn(
                "dkArea",
                F.when(F.col("municipalityCode") > 400, 1).otherwise(2)
            )
        
        # Aggregate consumption by timestamp and dkArea
        consumption_agg = consumption_forecast.groupBy("timestamp", "dkArea").agg(
            F.sum("consumptionkWh").alias("total_consumption")
        )
        
        # Aggregate production by timestamp and dkArea
        production_agg = production_forecast.groupBy(
            F.col("timeObserved").alias("timestamp"),
            "dkArea"
        ).agg(
            F.sum("productionKwh").alias("total_production"),
            F.sum("windProductionKwh").alias("wind_production"),
            F.sum("sunProductionKwh").alias("solar_production")
        )
        
        # Merge
        merged = consumption_agg.join(
            production_agg,
            on=["timestamp", "dkArea"],
            how="inner"
        )
        
        # Add time features
        merged = self._create_time_features(merged)
        
        # Add derived features
        merged = merged.withColumn(
            "production_ratio",
            F.when(F.col("total_consumption") > 0,
                  F.col("total_production") / F.col("total_consumption")
            ).otherwise(0.0)
        )
        
        merged = merged.withColumn(
            "net_demand",
            F.col("total_consumption") - F.col("total_production")
        )
        
        # Fill nulls
        merged = merged.fillna(0.0)
        
        return merged
    
    def _create_time_features(self, df: DataFrame) -> DataFrame:
        """Create time-based features"""
        
        df = df.withColumn("hour", F.hour("timestamp"))
        df = df.withColumn("day_of_week", F.dayofweek("timestamp") - 1)
        df = df.withColumn("month", F.month("timestamp"))
        df = df.withColumn("day_of_year", F.dayofyear("timestamp"))
        df = df.withColumn("is_weekend", 
                          F.when(F.col("day_of_week") >= 5, 1).otherwise(0))
        
        # Cyclical encoding
        df = df.withColumn("hour_sin", 
                          F.sin(F.lit(2 * math.pi) * F.col("hour") / 24))
        df = df.withColumn("hour_cos", 
                          F.cos(F.lit(2 * math.pi) * F.col("hour") / 24))
        
        df = df.withColumn("month_sin", 
                          F.sin(F.lit(2 * math.pi) * F.col("month") / 12))
        df = df.withColumn("month_cos", 
                          F.cos(F.lit(2 * math.pi) * F.col("month") / 12))
        
        return df
    
    def _predict_single_area(
        self,
        forecast_df: DataFrame,
        dk_area: int
    ) -> pd.DataFrame:
        """
        Predict prices for a single DK area
        
        Args:
            forecast_df: Forecast DataFrame with all features
            dk_area: DK area (1 or 2)
        
        Returns:
            Pandas DataFrame with predictions
        """
        if dk_area not in self.models:
            logger.error(f"No model for DK{dk_area}")
            raise ValueError(f"No model available for DK{dk_area}")
        
        model = self.models[dk_area]
        
        # Filter to single area
        area_df = forecast_df.filter(F.col("dkArea") == dk_area)
        
        # Disable Arrow for pandas conversion to avoid compatibility issues
        spark = self.spark
        arrow_enabled = spark.conf.get("spark.sql.execution.arrow.pyspark.enabled", "false")
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        
        try:
            # Convert to pandas
            area_pd = area_df.select(["timestamp"] + self.feature_columns).toPandas()
        finally:
            # Restore Arrow setting
            spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", arrow_enabled)
        
        if len(area_pd) == 0:
            logger.warning(f"No forecast data for DK{dk_area}")
            return pd.DataFrame(columns=['timestamp', 'dkArea', 'price'])
        
        # Generate predictions
        X_forecast = area_pd[self.feature_columns].apply(pd.to_numeric, errors='coerce').fillna(0)
        predictions = model.predict(X_forecast.values)
        
        # FIX: Apply minimum price constraint to prevent negative prices
        # Electricity prices can theoretically be negative in rare cases (oversupply),
        # but typically should be >= 0. Adjust this threshold based on your domain knowledge.
        MIN_PRICE = 0.0  # EUR/MWh - adjust if negative prices are valid in your market
        predictions = predictions.clip(min=MIN_PRICE)
        
        # Create results
        result_df = pd.DataFrame({
            'timestamp': area_pd['timestamp'],
            'dkArea': dk_area,
            'price': predictions
        })
        
        # Log statistics including any originally negative predictions
        original_predictions = model.predict(X_forecast.values)
        num_negative = (original_predictions < 0).sum()
        
        if num_negative > 0:
            logger.warning(f"  DK{dk_area}: {num_negative} predictions were negative (clipped to {MIN_PRICE})")
            logger.warning(f"    Original range: {original_predictions.min():.2f} to {original_predictions.max():.2f}")
        
        logger.info(f"  DK{dk_area}: {len(result_df):,} predictions, "
                   f"avg price: {result_df['price'].mean():.2f} EUR/MWh, "
                   f"range: {result_df['price'].min():.2f} - {result_df['price'].max():.2f}")
        
        return result_df