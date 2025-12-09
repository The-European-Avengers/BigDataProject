"""
Model training logic using Spark and XGBoost
"""

import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging
from typing import Dict, Tuple

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
    """Trains XGBoost model for energy consumption prediction"""
    
    def __init__(self):
        self.model = None
        self.trend_multipliers = {}
        self.feature_columns = settings.model.feature_columns
    
    def train(self, training_data: Dict[str, DataFrame]) -> Tuple[xgb.XGBRegressor, Dict[int, float]]:
        """
        Train the model
        
        Args:
            training_data: Dictionary with 'consumption', 'temp', 'sun', 'wind' DataFrames
        
        Returns:
            Tuple of (trained_model, trend_multipliers)
        """
        logger.info("=" * 80)
        logger.info("TRAINING PHASE")
        logger.info("=" * 80)
        
        # Prepare training data
        training_df = self._prepare_training_data(training_data)
        
        # Convert to pandas for XGBoost
        logger.info("Converting to pandas for XGBoost training...")
        training_pd = training_df.select(
            self.feature_columns + ["consumptionKwh"]
        ).toPandas()
        
        logger.info(f"Training dataset size: {len(training_pd):,} records")
        
        # Prepare features and target
        X = training_pd[self.feature_columns]
        y = training_pd['consumptionKwh']
        
        # Train-validation split (80-20)
        split_idx = int(len(X) * 0.8)
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y[:split_idx], y[split_idx:]
        
        logger.info(f"Training samples: {len(X_train):,}")
        logger.info(f"Validation samples: {len(X_val):,}")
        
        # Train XGBoost model
        logger.info("\nTraining XGBoost model...")
        self.model = xgb.XGBRegressor(
            n_estimators=settings.model.n_estimators,
            learning_rate=settings.model.learning_rate,
            max_depth=settings.model.max_depth,
            min_child_weight=settings.model.min_child_weight,
            subsample=settings.model.subsample,
            colsample_bytree=settings.model.colsample_bytree,
            random_state=settings.model.random_state,
            n_jobs=-1
        )
        
        self.model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False
        )
        
        # Validation metrics
        y_pred_val = self.model.predict(X_val)
        mae = mean_absolute_error(y_val, y_pred_val)
        rmse = np.sqrt(mean_squared_error(y_val, y_pred_val))
        mape = np.mean(np.abs((y_val - y_pred_val) / y_val)) * 100
        
        logger.info("\n" + "=" * 80)
        logger.info("Validation Metrics")
        logger.info("=" * 80)
        logger.info(f"MAE:  {mae:.2f} kWh")
        logger.info(f"RMSE: {rmse:.2f} kWh")
        logger.info(f"MAPE: {mape:.2f}%")
        
        # Feature importance
        self._log_feature_importance()
        
        # Calculate trend multipliers
        self.trend_multipliers = TrendCalculator.calculate_ytd_trend(training_df)
        
        return self.model, self.trend_multipliers
    
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
    
    def _log_feature_importance(self):
        """Log feature importance"""
        logger.info("\n" + "=" * 80)
        logger.info("Top 10 Feature Importances")
        logger.info("=" * 80)
        
        feature_imp = pd.DataFrame({
            'feature': self.feature_columns,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        for idx, row in feature_imp.head(10).iterrows():
            logger.info(f"{row['feature']:30s} {row['importance']:.4f}")