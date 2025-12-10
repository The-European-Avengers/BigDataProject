"""
Model training logic using Spark and XGBoost
Trains separate models per municipality to reduce memory usage
"""

import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging
from typing import Dict, Tuple, List, Optional

from src.config.settings import settings
from src.features.engineering import FeatureEngineering

logger = logging.getLogger(__name__)


class TrendCalculator:
    """Calculates year-over-year trend adjustment"""
    
    @staticmethod
    def calculate_ytd_trend(
        training_df: DataFrame
    ) -> Dict[int, float]:
        """
        Calculate year-over-year trend multiplier for each municipality
        
        Args:
            training_df: Training DataFrame with all years
        
        Returns:
            Dictionary mapping municipality_code -> trend_multiplier
        """
        logger.info("Calculating year-over-year trend multipliers...")
        
        # Get the latest year in the data
        max_year = training_df.select(F.max(F.year("timeDK"))).collect()[0][0]
        
        # Calculate average consumption by municipality and year
        yearly_avg = training_df.groupBy(
            "municipalityCode",
            F.year("timeDK").alias("year")
        ).agg(
            F.avg("consumptionKwh").alias("avg_consumption")
        )
        
        # Get latest year and previous years
        latest_year = yearly_avg.filter(F.col("year") == max_year)
        previous_years = yearly_avg.filter(F.col("year") < max_year)
        
        # Calculate average for previous years
        prev_avg = previous_years.groupBy("municipalityCode").agg(
            F.avg("avg_consumption").alias("historical_avg")
        )
        
        # Join and calculate trend
        trends = latest_year.join(
            prev_avg,
            on="municipalityCode",
            how="left"
        ).withColumn(
            "trend",
            F.when(
                F.col("historical_avg") > 0,
                F.col("avg_consumption") / F.col("historical_avg")
            ).otherwise(1.0)
        )
        
        # Convert to dictionary
        trend_dict = {
            int(row.municipalityCode): float(row.trend)
            for row in trends.select("municipalityCode", "trend").collect()
        }
        
        if trend_dict:
            logger.info(f"Calculated trends for {len(trend_dict)} municipalities")
            trend_values = list(trend_dict.values())
            logger.info(f"Trend range: {min(trend_values):.2f} - {max(trend_values):.2f}")
        else:
            logger.warning("No trends calculated, using default 1.0")
        
        return trend_dict


class ModelTrainer:
    """Trains XGBoost models per municipality for energy consumption prediction"""
    
    def __init__(self):
        self.models = {}  # Dictionary of municipality_code -> model
        self.global_model = None  # Fallback model for municipalities with insufficient data
        self.trend_multipliers = {}
        self.feature_columns = settings.model.feature_columns
        self.min_records_threshold = 1000  # Minimum records needed to train municipality-specific model
    
    def train(self, training_data: Dict[str, DataFrame]) -> Tuple[Dict[int, xgb.XGBRegressor], Dict[int, float]]:
        """
        Train separate models for each municipality with global fallback
        
        Args:
            training_data: Dictionary with 'consumption', 'temp', 'sun', 'wind' DataFrames
        
        Returns:
            Tuple of (dict of trained_models, trend_multipliers)
        """
        logger.info("=" * 80)
        logger.info("TRAINING PHASE - PER MUNICIPALITY WITH GLOBAL FALLBACK")
        logger.info("=" * 80)
        
        # Prepare training data
        training_df = self._prepare_training_data(training_data)
        
        # Get list of municipalities
        municipalities = [row.municipalityCode for row in 
                         training_df.select("municipalityCode").distinct().collect()]
        municipalities.sort()
        
        logger.info(f"Found {len(municipalities)} municipalities to train")
        
        # First, train global model as fallback
        logger.info(f"\n{'='*60}")
        logger.info("Training GLOBAL FALLBACK MODEL on all data")
        logger.info(f"{'='*60}")
        self.global_model = self._train_global_model(training_df)
        logger.info("✓ Global fallback model trained successfully")
        
        # Train model for each municipality
        municipalities_with_model = 0
        municipalities_using_fallback = 0
        
        for idx, muni_code in enumerate(municipalities, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Training municipality {muni_code} ({idx}/{len(municipalities)})")
            logger.info(f"{'='*60}")
            
            try:
                model = self._train_single_municipality(training_df, muni_code)
                if model is not None:
                    self.models[muni_code] = model
                    municipalities_with_model += 1
                    logger.info(f"✓ Successfully trained specific model for municipality {muni_code}")
                else:
                    # Use global model as fallback
                    self.models[muni_code] = self.global_model
                    municipalities_using_fallback += 1
                    logger.info(f"→ Using global fallback model for municipality {muni_code}")
            except Exception as e:
                logger.error(f"✗ Failed to train municipality {muni_code}: {e}")
                # Use global model as fallback
                self.models[muni_code] = self.global_model
                municipalities_using_fallback += 1
                logger.info(f"→ Using global fallback model for municipality {muni_code}")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Training Complete:")
        logger.info(f"  - Municipalities with specific models: {municipalities_with_model}")
        logger.info(f"  - Municipalities using global fallback: {municipalities_using_fallback}")
        logger.info(f"  - Total: {len(self.models)}/{len(municipalities)}")
        logger.info(f"{'='*80}")
        
        # Calculate trend multipliers (using full dataset)
        self.trend_multipliers = TrendCalculator.calculate_ytd_trend(training_df)
        
        return self.models, self.trend_multipliers
    
    def _train_global_model(self, training_df: DataFrame) -> xgb.XGBRegressor:
        """
        Train a global model on all municipalities
        
        Args:
            training_df: Full training DataFrame with all features
        
        Returns:
            Trained XGBoost model
        """
        # Sample data to avoid OOM (use 20% of data for global model)
        sampled_df = training_df.sample(fraction=0.2, seed=42)
        
        # Convert to pandas
        global_pd = sampled_df.select(
            self.feature_columns + ["consumptionKwh"]
        ).toPandas()
        
        n_records = len(global_pd)
        logger.info(f"  Training on sampled dataset: {n_records:,} records")
        
        # Prepare features and target
        X = global_pd[self.feature_columns]
        y = global_pd['consumptionKwh']
        
        # Train-validation split (80-20)
        split_idx = int(len(X) * 0.8)
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y[:split_idx], y[split_idx:]
        
        logger.info(f"  Training: {len(X_train):,} | Validation: {len(X_val):,}")
        
        # Train XGBoost model
        model = xgb.XGBRegressor(
            n_estimators=settings.model.n_estimators,
            learning_rate=settings.model.learning_rate,
            max_depth=settings.model.max_depth,
            min_child_weight=settings.model.min_child_weight,
            subsample=settings.model.subsample,
            colsample_bytree=settings.model.colsample_bytree,
            random_state=settings.model.random_state,
            n_jobs=-1
        )
        
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False
        )
        
        # Validation metrics
        y_pred_val = model.predict(X_val)
        mae = mean_absolute_error(y_val, y_pred_val)
        rmse = np.sqrt(mean_squared_error(y_val, y_pred_val))
        mape = np.mean(np.abs((y_val - y_pred_val) / y_val)) * 100
        
        logger.info(f"  MAE: {mae:.2f} kWh | RMSE: {rmse:.2f} kWh | MAPE: {mape:.2f}%")
        
        return model
    
    def _train_single_municipality(
        self, 
        training_df: DataFrame, 
        muni_code: int
    ) -> Optional[xgb.XGBRegressor]:
        """
        Train model for a single municipality
        
        Args:
            training_df: Full training DataFrame with all features
            muni_code: Municipality code to train
        
        Returns:
            Trained XGBoost model or None if insufficient data
        """
        # Filter to single municipality
        muni_df = training_df.filter(F.col("municipalityCode") == muni_code)
        
        # Convert to pandas
        muni_pd = muni_df.select(
            self.feature_columns + ["consumptionKwh"]
        ).toPandas()
        
        n_records = len(muni_pd)
        logger.info(f"  Records: {n_records:,}")
        
        # Check if sufficient data
        if n_records < self.min_records_threshold:
            logger.warning(f"  Insufficient data ({n_records} < {self.min_records_threshold}), will use global model")
            return None
        
        # Prepare features and target
        X = muni_pd[self.feature_columns]
        y = muni_pd['consumptionKwh']
        
        # Train-validation split (80-20)
        split_idx = int(len(X) * 0.8)
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y[:split_idx], y[split_idx:]
        
        logger.info(f"  Training: {len(X_train):,} | Validation: {len(X_val):,}")
        
        # Train XGBoost model
        model = xgb.XGBRegressor(
            n_estimators=settings.model.n_estimators,
            learning_rate=settings.model.learning_rate,
            max_depth=settings.model.max_depth,
            min_child_weight=settings.model.min_child_weight,
            subsample=settings.model.subsample,
            colsample_bytree=settings.model.colsample_bytree,
            random_state=settings.model.random_state,
            n_jobs=-1
        )
        
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False
        )
        
        # Validation metrics
        y_pred_val = model.predict(X_val)
        mae = mean_absolute_error(y_val, y_pred_val)
        rmse = np.sqrt(mean_squared_error(y_val, y_pred_val))
        mape = np.mean(np.abs((y_val - y_pred_val) / y_val)) * 100
        
        logger.info(f"  MAE: {mae:.2f} kWh | RMSE: {rmse:.2f} kWh | MAPE: {mape:.2f}%")
        
        return model
    
    def _prepare_training_data(self, training_data: Dict[str, DataFrame]) -> DataFrame:
        """Prepare training data with feature engineering"""
        
        logger.info("Preparing training data...")
        
        consumption_df = training_data['consumption']
        temp_df = training_data['temp']
        sun_df = training_data['sun']
        
        # Merge and engineer features
        merged_df = FeatureEngineering.merge_consumption_weather(
            consumption_df, temp_df, sun_df
        )
        merged_df = FeatureEngineering.create_time_features(merged_df, "timeDK")
        merged_df = FeatureEngineering.create_interaction_features(merged_df)
        merged_df = FeatureEngineering.create_lag_features(merged_df)
        
        # Remove rows with nulls
        merged_df = merged_df.dropna()
        
        count = merged_df.count()
        logger.info(f"Training data prepared: {count:,} records")
        
        return merged_df
    
    def _log_feature_importance(self, model: xgb.XGBRegressor, muni_code: int):
        """Log feature importance for a municipality"""
        logger.info(f"\nTop 10 Features for Municipality {muni_code}")
        logger.info("-" * 60)
        
        feature_imp = pd.DataFrame({
            'feature': self.feature_columns,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        for idx, row in feature_imp.head(10).iterrows():
            logger.info(f"  {row['feature']:30s} {row['importance']:.4f}")