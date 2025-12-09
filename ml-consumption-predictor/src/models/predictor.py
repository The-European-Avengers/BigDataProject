"""
Prediction generation logic using Spark
"""

import xgboost as xgb
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pandas as pd
import logging
from typing import Dict

from src.config.settings import settings
from src.features.engineering import FeatureEngineering
from src.utils.spark_utils import get_spark

logger = logging.getLogger(__name__)


class EnergyPredictor:
    """Generates energy consumption predictions using Spark"""
    
    def __init__(
        self,
        model: xgb.XGBRegressor,
        trend_multipliers: Dict[int, float],
        historical_df: DataFrame
    ):
        """
        Initialize predictor
        
        Args:
            model: Trained XGBoost model
            trend_multipliers: Municipality-specific trend adjustments
            historical_df: Historical data for lag features
        """
        self.model = model
        self.trend_multipliers = trend_multipliers
        self.historical_df = historical_df
        self.spark = get_spark()
        self.feature_columns = settings.model.feature_columns
    
    def predict(
        self,
        temp_forecast: DataFrame,
        sun_forecast: DataFrame
    ) -> DataFrame:
        """
        Generate predictions for forecast period
        
        Args:
            temp_forecast: Temperature forecast DataFrame (has mean_temp or value column)
            sun_forecast: Sunlight forecast DataFrame (has mean_radiation or value column)
        
        Returns:
            Spark DataFrame with predictions including weather features
        """
        logger.info("=" * 80)
        logger.info("PREDICTION PHASE")
        logger.info("=" * 80)
        
        # Merge weather forecasts
        forecast_df = self._merge_forecast_weather(temp_forecast, sun_forecast)
        
        # Prepare features
        forecast_with_features = FeatureEngineering.prepare_forecast_features(
            forecast_df,
            self.historical_df
        )
        
        # Convert to pandas for prediction
        logger.info("Preparing data for prediction...")
        
        # Select required columns for prediction + weather data for output
        select_cols = ["timeDK", "municipalityCode", "temperature", "sunlight"] + self.feature_columns
        forecast_pd = forecast_with_features.select(select_cols).toPandas()
        
        logger.info(f"Predicting for {len(forecast_pd):,} records...")
        
        # Generate base predictions
        X_forecast = forecast_pd[self.feature_columns]
        base_predictions = self.model.predict(X_forecast)
        
        # Apply trend adjustment
        adjusted_predictions = []
        
        for idx, row in forecast_pd.iterrows():
            muni_code = int(row['municipalityCode'])
            trend = self.trend_multipliers.get(muni_code, 1.0)
            adjusted_pred = base_predictions[idx] * trend
            adjusted_predictions.append(adjusted_pred)
        
        # Create results DataFrame with all required columns
        forecast_pd['consumptionkWh'] = adjusted_predictions
        forecast_pd['productionkWh'] = 0.0  # Placeholder
        forecast_pd['price'] = 0.0  # Placeholder
        
        # Rename columns for output
        output_pd = forecast_pd.rename(columns={
            'timeDK': 'timestamp',
            'temperature': 'mean_temp',
            'sunlight': 'mean_radiation'
        })
        
        # Add wind speed (set to 0 if not available)
        if 'mean_wind_speed' not in output_pd.columns:
            output_pd['mean_wind_speed'] = 0.0
        
        # Select final output columns
        output_cols = [
            'timestamp',
            'municipalityCode',
            'consumptionkWh',
            'mean_temp',
            'mean_radiation',
            'mean_wind_speed',
            'productionkWh',
            'price'
        ]
        output_pd = output_pd[output_cols]
        
        # Convert back to Spark DataFrame
        result_df = self.spark.createDataFrame(output_pd)
        
        logger.info(f"Generated {result_df.count():,} predictions")
        
        # Log summary
        self._log_prediction_summary(result_df)
        
        return result_df
    
    def _merge_forecast_weather(
        self,
        temp_df: DataFrame,
        sun_df: DataFrame
    ) -> DataFrame:
        """Merge temperature and sunlight forecasts"""
        
        logger.info("Merging forecast weather data...")
        
        # Aggregate by timestamp and municipality
        temp_agg = temp_df.groupBy("timestamp", "municipalityCode") \
            .agg(F.avg("value").alias("temperature"))
        
        sun_agg = sun_df.groupBy("timestamp", "municipalityCode") \
            .agg(F.avg("value").alias("sunlight"))
        
        # Merge
        merged = temp_agg.join(
            sun_agg,
            on=["timestamp", "municipalityCode"],
            how="outer"
        )
        
        # Fill missing values
        merged = merged.fillna({
            'temperature': 10.0,
            'sunlight': 0.0
        })
        
        return merged
    
    def _log_prediction_summary(self, predictions_df: DataFrame):
        """Log summary statistics"""
        
        logger.info("\n" + "=" * 80)
        logger.info("Prediction Summary")
        logger.info("=" * 80)
        
        stats = predictions_df.select(
            F.min("timestamp").alias("min_time"),
            F.max("timestamp").alias("max_time"),
            F.countDistinct("municipalityCode").alias("num_munis"),
            F.avg("consumptionkWh").alias("avg_consumption"),
            F.min("consumptionkWh").alias("min_consumption"),
            F.max("consumptionkWh").alias("max_consumption")
        ).collect()[0]
        
        logger.info(f"Time range: {stats.min_time} to {stats.max_time}")
        logger.info(f"Municipalities: {stats.num_munis}")
        logger.info(f"Avg consumption: {stats.avg_consumption:.2f} kWh")
        logger.info(f"Range: {stats.min_consumption:.2f} - {stats.max_consumption:.2f} kWh")