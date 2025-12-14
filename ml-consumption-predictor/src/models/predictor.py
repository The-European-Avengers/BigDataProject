"""
Prediction generation logic using Spark
Predicts per municipality using separate models
"""

import xgboost as xgb
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pandas as pd
import logging
from typing import Dict, Optional

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
        
        # Debug: Verify feature_columns is a proper list of strings
        logger.info(f"Feature columns type: {type(self.feature_columns)}")
        logger.info(f"Number of feature columns: {len(self.feature_columns)}")
        logger.info(f"Feature columns: {self.feature_columns}")
        
        # Check for duplicates
        if len(self.feature_columns) != len(set(self.feature_columns)):
            duplicates = [col for col in self.feature_columns if self.feature_columns.count(col) > 1]
            logger.warning(f"Duplicate columns found: {set(duplicates)}")
        
        # Check all are strings
        non_strings = [col for col in self.feature_columns if not isinstance(col, str)]
        if non_strings:
            logger.error(f"Non-string column names found: {non_strings}")
            raise ValueError(f"Feature columns must all be strings, found: {non_strings}")
    
    def predict(
        self,
        temp_forecast: DataFrame,
        sun_forecast: DataFrame,
        wind_forecast: Optional[DataFrame] = None  # FIX: Add wind parameter
    ) -> DataFrame:
        """
        Generate predictions for forecast period
        
        Args:
            temp_forecast: Temperature forecast DataFrame
            sun_forecast: Sunlight forecast DataFrame
            wind_forecast: Wind speed forecast DataFrame (optional)
        
        Returns:
            Spark DataFrame with predictions including weather features
        """
        logger.info("=" * 80)
        logger.info("PREDICTION PHASE - PER MUNICIPALITY")
        logger.info("=" * 80)
        
        # Debug: Check forecast data before merging
        logger.info("Checking forecast data ranges...")
        temp_dates = temp_forecast.select(
            F.min("timestamp").alias("min_ts"),
            F.max("timestamp").alias("max_ts"),
            F.count("*").alias("count")
        ).collect()[0]
        logger.info(f"Temperature forecast: {temp_dates.min_ts} to {temp_dates.max_ts} ({temp_dates.count} records)")
        
        sun_dates = sun_forecast.select(
            F.min("timestamp").alias("min_ts"),
            F.max("timestamp").alias("max_ts"),
            F.count("*").alias("count")
        ).collect()[0]
        logger.info(f"Sunlight forecast: {sun_dates.min_ts} to {sun_dates.max_ts} ({sun_dates.count} records)")
        
        # FIX: Merge weather forecasts including wind
        forecast_df = self._merge_forecast_weather(temp_forecast, sun_forecast, wind_forecast)
        
        # Debug: Check after merge
        merged_dates = forecast_df.select(
            F.min("timestamp").alias("min_ts"),
            F.max("timestamp").alias("max_ts"),
            F.count("*").alias("count"),
            F.countDistinct("timestamp").alias("distinct_ts")
        ).collect()[0]
        logger.info(f"After merge: {merged_dates.min_ts} to {merged_dates.max_ts} ({merged_dates.count} records, {merged_dates.distinct_ts} unique timestamps)")
        
        # Prepare features
        forecast_with_features = FeatureEngineering.prepare_forecast_features(
            forecast_df,
            self.historical_df
        )
        
        # Debug: Check after feature engineering
        feature_dates = forecast_with_features.select(
            F.min("timeDK").alias("min_ts"),
            F.max("timeDK").alias("max_ts"),
            F.count("*").alias("count"),
            F.countDistinct("timeDK").alias("distinct_ts")
        ).collect()[0]
        logger.info(f"After features: {feature_dates.min_ts} to {feature_dates.max_ts} ({feature_dates.count} records, {feature_dates.distinct_ts} unique timestamps)")
        
        # Get list of municipalities in forecast
        municipalities = [row.municipalityCode for row in 
                         forecast_with_features.select("municipalityCode").distinct().collect()]
        municipalities.sort()
        
        logger.info(f"Predicting for {len(municipalities)} municipalities...")
        
        # Predict for each municipality
        all_predictions = []
        
        for idx, muni_code in enumerate(municipalities, 1):
            try:
                muni_predictions = self._predict_single_municipality(
                    forecast_with_features, 
                    muni_code
                )
                all_predictions.append(muni_predictions)
                logger.info(f"  [{idx}/{len(municipalities)}] Municipality {muni_code}: ✓ {len(muni_predictions):,} predictions")
            except Exception as e:
                logger.error(f"  [{idx}/{len(municipalities)}] Municipality {muni_code}: ✗ Failed: {e}")
                continue
        
        if not all_predictions:
            raise ValueError("No predictions generated for any municipality")
        
        # Combine all predictions
        logger.info("\nCombining predictions from all municipalities...")
        result_pd = pd.concat(all_predictions, ignore_index=True)
        
        # Debug: Check combined predictions
        logger.info(f"Combined pandas DataFrame: {len(result_pd)} rows")
        logger.info(f"Unique timestamps in result: {result_pd['timestamp'].nunique()}")
        logger.info(f"Timestamp range: {result_pd['timestamp'].min()} to {result_pd['timestamp'].max()}")
        
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
        try:
            # Check if we have a model for this municipality
            if muni_code not in self.models:
                logger.warning(f"No model for municipality {muni_code}, skipping")
                raise ValueError(f"No model available for municipality {muni_code}")
            
            model = self.models[muni_code]
            
            # Filter to single municipality
            muni_df = forecast_df.filter(F.col("municipalityCode") == muni_code)
            
            # Check available columns
            available_cols = set(muni_df.columns)
            
            # FIX: Include weather columns for output
            required_cols_for_output = ["timeDK", "temperature", "sunlight", "wind_speed"]
            required_cols = list(set(required_cols_for_output + self.feature_columns))
            
            missing_cols = set(required_cols) - available_cols
            
            if missing_cols:
                logger.error(f"Municipality {muni_code}: Missing columns: {missing_cols}")
                logger.error(f"Available columns: {sorted(available_cols)}")
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Convert to Pandas DataFrame - select only what we need
            muni_pd = muni_df.select(required_cols).toPandas()
            
            if len(muni_pd) == 0:
                raise ValueError(f"No forecast data for municipality {muni_code}")
            
            # Generate base predictions - use ONLY feature columns
            X_forecast = muni_pd[self.feature_columns].copy()
            
            # Debug: Log column information
            logger.debug(f"X_forecast shape: {X_forecast.shape}")
            logger.debug(f"X_forecast columns: {list(X_forecast.columns)}")
            logger.debug(f"X_forecast dtypes:\n{X_forecast.dtypes}")
            
            # Ensure X_forecast is all numeric and handle any NaN values
            X_forecast = X_forecast.apply(pd.to_numeric, errors='coerce').fillna(0)
            
            # Verify it's a proper DataFrame with no nested structures
            if not isinstance(X_forecast, pd.DataFrame):
                raise ValueError(f"X_forecast is not a DataFrame, it's {type(X_forecast)}")
            
            # Convert to numpy array to avoid any pandas indexing issues with XGBoost
            X_array = X_forecast.values
            
            base_predictions = model.predict(X_array)
            
        except Exception as e:
            logger.error(f"Error in _predict_single_municipality for {muni_code}: {str(e)}", exc_info=True)
            raise
        
        # Apply trend adjustment
        trend = self.trend_multipliers.get(muni_code, 1.0)
        adjusted_predictions = base_predictions * trend
        
        # FIX: Use actual weather values from forecast instead of hardcoded 0.0
        # CRITICAL: Remove microseconds from timestamps to ensure proper joins
        timestamps = pd.to_datetime(muni_pd['timeDK']).dt.floor('s')
        
        result_df = pd.DataFrame({
            'timestamp': timestamps,
            'municipalityCode': muni_pd['municipalityCode'],
            'consumptionkWh': adjusted_predictions,
            'mean_temp': muni_pd['temperature'],
            'mean_radiation': muni_pd['sunlight'],
            'mean_wind_speed': muni_pd.get('wind_speed', 0.0),  # Use actual wind or 0.0 if missing
            'productionkWh': 0.0,  # Placeholder - will be filled by production calculator
            'price': 0.0  # Placeholder - will be filled by price predictor
        })
        
        return result_df
    
    def _merge_forecast_weather(
        self,
        temp_df: DataFrame,
        sun_df: DataFrame,
        wind_df: Optional[DataFrame] = None  # FIX: Add wind parameter
    ) -> DataFrame:
        """Merge temperature, sunlight, and wind forecasts"""
        
        logger.info("Merging forecast weather data...")
        
        # FIX: First floor timestamps to seconds to remove any milliseconds
        temp_df = temp_df.withColumn("timestamp", F.date_trunc("second", F.col("timestamp")))
        sun_df = sun_df.withColumn("timestamp", F.date_trunc("second", F.col("timestamp")))
        if wind_df is not None:
            wind_df = wind_df.withColumn("timestamp", F.date_trunc("second", F.col("timestamp")))
        
        # Aggregate by timestamp and municipality (take average if multiple readings)
        temp_agg = temp_df.groupBy("timestamp", "municipalityCode") \
            .agg(F.avg("value").alias("temperature"))
        
        sun_agg = sun_df.groupBy("timestamp", "municipalityCode") \
            .agg(F.avg("value").alias("sunlight"))
        
        # FIX: Merge wind if available
        if wind_df is not None:
            wind_agg = wind_df.groupBy("timestamp", "municipalityCode") \
                .agg(F.avg("value").alias("wind_speed"))
            
            # Merge all three
            merged = temp_agg.join(
                sun_agg,
                on=["timestamp", "municipalityCode"],
                how="outer"
            ).join(
                wind_agg,
                on=["timestamp", "municipalityCode"],
                how="outer"
            )
        else:
            # Merge temp and sun only
            merged = temp_agg.join(
                sun_agg,
                on=["timestamp", "municipalityCode"],
                how="outer"
            )
            # Add wind_speed column with 0.0 if no wind data
            merged = merged.withColumn("wind_speed", F.lit(0.0))
        
        # Fill missing values
        merged = merged.fillna({
            'temperature': 10.0,
            'sunlight': 0.0,
            'wind_speed': 0.0
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