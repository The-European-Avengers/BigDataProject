"""
Price model training logic
Trains separate XGBoost models for DK1 and DK2
"""

import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging
import math
from typing import Dict, Tuple

from src.config.settings import settings

logger = logging.getLogger(__name__)


class PriceModelTrainer:
    """Trains XGBoost models for price prediction per DK area"""
    
    def __init__(self):
        self.models = {}  # Dictionary of dkArea -> model
        self.feature_columns = settings.model.price_feature_columns
    
    def train(
        self,
        consumption_df: DataFrame,
        production_df: DataFrame,
        price_df: DataFrame
    ) -> Dict[int, xgb.XGBRegressor]:
        """
        Train price models for DK1 and DK2
        
        Args:
            consumption_df: Historical consumption (timeDK, municipalityCode, dkArea, consumptionKwh)
            production_df: Historical production (timeObserved, municipalityCode, dkArea, 
                                                 windProductionKwh, sunProductionKwh, productionKwh)
            price_df: Historical prices (timestamp, dkArea, price_EUR_MWh)
        
        Returns:
            Dictionary mapping dkArea -> trained model
        """
        logger.info("=" * 80)
        logger.info("PRICE MODEL TRAINING - DK1 and DK2")
        logger.info("=" * 80)
        
        # Prepare training data
        training_df = self._prepare_price_training_data(
            consumption_df, 
            production_df, 
            price_df
        )
        
        # Train model for DK1
        logger.info(f"\n{'='*60}")
        logger.info("Training DK1 Price Model")
        logger.info(f"{'='*60}")
        self.models[1] = self._train_single_area(training_df, dk_area=1)
        
        # Train model for DK2
        logger.info(f"\n{'='*60}")
        logger.info("Training DK2 Price Model")
        logger.info(f"{'='*60}")
        self.models[2] = self._train_single_area(training_df, dk_area=2)
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Price Model Training Complete")
        logger.info(f"{'='*80}")
        
        return self.models
    
    def _prepare_price_training_data(
        self,
        consumption_df: DataFrame,
        production_df: DataFrame,
        price_df: DataFrame
    ) -> DataFrame:
        """
        Prepare aggregated training data for price models
        
        Returns:
            DataFrame with features: timestamp, dkArea, total_consumption, total_production,
                                   wind_production, solar_production, time features, price
        """
        logger.info("Preparing price training data...")
        
        # Ensure dkArea exists in consumption (infer if missing)
        if "dkArea" not in consumption_df.columns:
            consumption_df = consumption_df.withColumn(
                "dkArea",
                F.when(F.col("municipalityCode") > 400, 1).otherwise(2)
            )
        
        # Aggregate consumption by timestamp and dkArea
        consumption_agg = consumption_df.groupBy(
            F.col("timeDK").alias("timestamp"),
            "dkArea"
        ).agg(
            F.sum("consumptionKwh").alias("total_consumption")
        )
        
        # Aggregate production by timestamp and dkArea
        production_agg = production_df.groupBy(
            F.col("timeObserved").alias("timestamp"),
            "dkArea"
        ).agg(
            F.sum("productionKwh").alias("total_production"),
            F.sum("windProductionKwh").alias("wind_production"),
            F.sum("sunProductionKwh").alias("solar_production")
        )
        
        # Merge consumption and production
        merged = consumption_agg.join(
            production_agg,
            on=["timestamp", "dkArea"],
            how="inner"
        )
        
        # Merge with price
        merged = merged.join(
            price_df,
            on=["timestamp", "dkArea"],
            how="inner"
        )
        
        # Rename price column
        merged = merged.withColumnRenamed("price_EUR_MWh", "price")
        
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
        
        # Remove nulls
        merged = merged.dropna()
        
        count = merged.count()
        logger.info(f"Price training data prepared: {count:,} records")
        
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
    
    def _train_single_area(
        self,
        training_df: DataFrame,
        dk_area: int
    ) -> xgb.XGBRegressor:
        """
        Train price model for a single DK area
        
        Args:
            training_df: Full training DataFrame
            dk_area: DK area (1 or 2)
        
        Returns:
            Trained XGBoost model
        """
        # Filter to single area
        area_df = training_df.filter(F.col("dkArea") == dk_area)
        
        # Disable Arrow for pandas conversion to avoid compatibility issues
        from src.utils.spark_utils import get_spark
        spark = get_spark()
        arrow_enabled = spark.conf.get("spark.sql.execution.arrow.pyspark.enabled", "false")
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        
        try:
            # Convert to pandas
            area_pd = area_df.select(
                self.feature_columns + ["price"]
            ).toPandas()
        finally:
            # Restore Arrow setting
            spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", arrow_enabled)
        
        n_records = len(area_pd)
        logger.info(f"  Records: {n_records:,}")
        
        if n_records < 100:
            logger.warning(f"  Insufficient data for DK{dk_area}")
            raise ValueError(f"Insufficient data for DK{dk_area}")
        
        # Prepare features and target
        X = area_pd[self.feature_columns]
        y = area_pd['price']
        
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
        mape = np.mean(np.abs((y_val - y_pred_val) / (y_val + 1e-6))) * 100  # Add small epsilon
        
        logger.info(f"  MAE: {mae:.2f} EUR/MWh | RMSE: {rmse:.2f} EUR/MWh | MAPE: {mape:.2f}%")
        
        return model