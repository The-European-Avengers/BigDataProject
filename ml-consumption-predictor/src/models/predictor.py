"""
Prediction generation logic using Spark
Predicts per municipality using separate models
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
        models: Dict[int, xgb.XGBRegressor],
        trend_multipliers: Dict[int, float],
        historical_df: DataFrame
    ):
        """
        Initialize predictor
        
        Args:
            models: Dictionary of municipality_code -> trained XGBoost model
            trend_multipliers: Municipality-specific trend adjustments
            historical_df: Historical data for lag features
        """
        self.models = models
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
            temp_forecast: Temperature forecast DataFrame
            sun_forecast: Sunlight forecast DataFrame
        
        Returns:
            Spark DataFrame with predictions including weather features
        """
        logger.info("=" * 80)
        logger.info("PREDICTION PHASE - PER MUNICIPALITY")
        logger.info("=" * 80)
        
        # Merge weather forecasts
        forecast_df = self._merge_forecast_weather(temp_forecast, sun_forecast)
        
        # Prepare features
        forecast_with_features = FeatureEngineering.prepare_forecast_features(
            forecast_df,
            self.historical_df
        )
        
        # Get list of municipalities in forecast
        municipalities = [row.municipalityCode for row in 
                         forecast_with_features.select("municipalityCode").distinct().collect()]
        municipalities.sort()
        
        logger.info(f"Predicting for {len(municipalities)} municipalities...")
        
        # Predict for each municipality
        all_predictions = []
        
        for idx, muni_code in enumerate(municipalities, 1):
            logger.info(f"  [{idx}/{len(municipalities)}] Municipality {muni_code}...", end=" ")
            
            try:
                muni_predictions = self._predict_single_municipality(
                    forecast_with_features, 
                    muni_code
                )
                all_predictions.append(muni_predictions)
                logger.info(f"✓ {len(muni_predictions):,} predictions")
            except Exception as e:
                logger.error(f"✗ Failed: {e}")
                continue
        
        if not all_predictions:
            raise ValueError("No predictions generated for any municipality")
        
        # Combine all predictions
        logger.info("\nCombining predictions from all municipalities...")
        result_pd = pd.concat(all_predictions, ignore_index=True)
        
        # Convert back to Spark DataFrame
        result_df = self.spark.createDataFrame(result_pd)
        
        logger.info(f"Generated {result_df.count():,} total predictions")
        
        # Log summary
        self._log_prediction_summary(result_df)
        
        return result_df
    
    def _predict_single_municipality(
        self,
        forecast_df: DataFrame,
        muni_code: int
    ) -> pd.DataFrame:
        """
        Generate predictions for a single municipality
        
        Args:
            forecast_df: Forecast DataFrame with all features
            muni_code: Municipality code
        
        Returns:
            Pandas DataFrame with predictions
        """
        # Check if we have a model for this municipality
        if muni_code not in self.models:
            logger.warning(f"No model for municipality {muni_code}, skipping")
            raise ValueError(f"No model available for municipality {muni_code}")
        
        model = self.models[muni_code]
        
        # Filter to single municipality
        muni_df = forecast_df.filter(F.col("municipalityCode") == muni_code)
        
        # Select required columns
        select_cols = ["timeDK", "municipalityCode", "temperature", "sunlight"] + self.feature_columns
        muni_pd = muni_df.select(select_cols).toPandas()
        
        if len(muni_pd) == 0:
            raise ValueError(f"No forecast data for municipality {muni_code}")
        
        # Generate base predictions
        X_forecast = muni_pd[self.feature_columns]
        base_predictions = model.predict(X_forecast)
        
        # Apply trend adjustment
        trend = self.trend_multipliers.get(muni_code, 1.0)
        adjusted_predictions = base_predictions * trend
        
        # Create results DataFrame
        muni_pd['consumptionkWh'] = adjusted_predictions
        muni_pd['productionkWh'] = 0.0  # Placeholder
        muni_pd['price'] = 0.0  # Placeholder
        
        # Rename columns for output
        output_pd = muni_pd.rename(columns={
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
        
        return output_pd[output_cols]
    
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