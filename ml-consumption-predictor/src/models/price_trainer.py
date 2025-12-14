"""
Price model training logic - SIMPLIFIED VERSION
No lag features, focus on what we can actually predict
"""

import xgboost as xgb
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging
import math
from typing import Dict

from src.config.settings import settings

logger = logging.getLogger(__name__)


class PriceModelTrainer:
    """Trains ensemble models for price prediction per DK area"""
    
    def __init__(self):
        self.models = {}  # Dictionary of dkArea -> model
        self.feature_columns = [
            # Time features (no lag!)
            'hour', 'day_of_week', 'month', 'day_of_year', 'is_weekend',
            'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
            # Supply/demand features
            'total_consumption', 'total_production', 
            'wind_production', 'solar_production',
            'production_ratio', 'net_demand', 'renewable_percentage',
            # Peak indicators
            'is_peak_hour', 'is_night_hour'
        ]
    
    def train(
        self,
        consumption_df: DataFrame,
        production_df: DataFrame,
        price_df: DataFrame
    ) -> Dict[int, xgb.XGBRegressor]:
        """
        Train price models for DK1 and DK2
        
        Args:
            consumption_df: Historical consumption
            production_df: Historical production
            price_df: Historical prices
        
        Returns:
            Dictionary mapping dkArea -> trained model
        """
        logger.info("=" * 80)
        logger.info("PRICE MODEL TRAINING - SIMPLIFIED APPROACH")
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
        """Prepare training data - NO LAG FEATURES"""
        
        logger.info("Preparing price training data (no lag features)...")
        
        # Ensure dkArea exists
        if "dkArea" not in consumption_df.columns:
            consumption_df = consumption_df.withColumn(
                "dkArea",
                F.when(F.col("municipalityCode") > 400, 1).otherwise(2)
            )
        
        # Aggregate consumption
        consumption_agg = consumption_df.groupBy(
            F.col("timeDK").alias("timestamp"),
            "dkArea"
        ).agg(
            F.sum("consumptionKwh").alias("total_consumption")
        )
        
        # Aggregate production
        production_agg = production_df.groupBy(
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
        
        # Merge with price
        merged = merged.join(
            price_df,
            on=["timestamp", "dkArea"],
            how="inner"
        )
        
        # Rename price column
        merged = merged.withColumnRenamed("price_EUR_MWh", "price")
        
        # Filter outliers
        merged = merged.filter(
            (F.col("price") >= -50) & (F.col("price") <= 500)
        )
        
        # Add features
        merged = self._create_time_features(merged)
        merged = self._add_derived_features(merged)
        
        # Remove nulls
        merged = merged.dropna()
        
        count = merged.count()
        logger.info(f"Training data: {count:,} records")
        
        # Price statistics
        price_stats = merged.select(
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.avg("price").alias("avg_price"),
            F.stddev("price").alias("std_price")
        ).collect()[0]
        
        logger.info(f"Price stats: min={price_stats.min_price:.1f}, "
                   f"max={price_stats.max_price:.1f}, "
                   f"avg={price_stats.avg_price:.1f} EUR/MWh")
        
        return merged
    
    def _create_time_features(self, df: DataFrame) -> DataFrame:
        """Create time features"""
        
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
    
    def _add_derived_features(self, df: DataFrame) -> DataFrame:
        """Add derived features"""
        
        df = df.withColumn(
            "production_ratio",
            F.when(F.col("total_consumption") > 0,
                  F.col("total_production") / F.col("total_consumption")
            ).otherwise(0.0)
        )
        
        df = df.withColumn(
            "net_demand",
            F.col("total_consumption") - F.col("total_production")
        )
        
        df = df.withColumn(
            "renewable_percentage",
            F.when(F.col("total_production") > 0,
                  (F.col("wind_production") + F.col("solar_production")) / F.col("total_production")
            ).otherwise(0.0)
        )
        
        df = df.withColumn(
            "is_peak_hour",
            F.when((F.col("hour") >= 17) & (F.col("hour") <= 20), 1).otherwise(0)
        )
        
        df = df.withColumn(
            "is_night_hour",
            F.when((F.col("hour") >= 0) & (F.col("hour") <= 6), 1).otherwise(0)
        )
        
        return df
    
    def _train_single_area(
        self,
        training_df: DataFrame,
        dk_area: int
    ) -> xgb.XGBRegressor:
        """Train model for single area - TRY MULTIPLE ALGORITHMS"""
        
        # Filter to single area
        area_df = training_df.filter(F.col("dkArea") == dk_area)
        
        # Disable Arrow
        from src.utils.spark_utils import get_spark
        spark = get_spark()
        arrow_enabled = spark.conf.get("spark.sql.execution.arrow.pyspark.enabled", "false")
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        
        try:
            area_pd = area_df.select(self.feature_columns + ["price"]).toPandas()
        finally:
            spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", arrow_enabled)
        
        n_records = len(area_pd)
        logger.info(f"  Records: {n_records:,}")
        logger.info(f"  Features: {len(self.feature_columns)}")
        
        if n_records < 100:
            raise ValueError(f"Insufficient data for DK{dk_area}")
        
        # Prepare data
        X = area_pd[self.feature_columns]
        y = area_pd['price']
        
        # Train-test split
        split_idx = int(len(X) * 0.8)
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y[:split_idx], y[split_idx:]
        
        logger.info(f"  Training: {len(X_train):,} | Validation: {len(X_val):,}")
        
        # Try multiple algorithms and pick the best
        models_to_test = {
            'XGBoost': xgb.XGBRegressor(
                n_estimators=200,
                learning_rate=0.05,
                max_depth=6,
                min_child_weight=3,
                subsample=0.8,
                colsample_bytree=0.8,
                gamma=0.1,
                reg_alpha=0.1,
                reg_lambda=1.0,
                random_state=42,
                n_jobs=-1
            ),
            'RandomForest': RandomForestRegressor(
                n_estimators=200,
                max_depth=15,
                min_samples_split=10,
                min_samples_leaf=5,
                random_state=42,
                n_jobs=-1
            ),
            'GradientBoosting': GradientBoostingRegressor(
                n_estimators=200,
                learning_rate=0.05,
                max_depth=6,
                min_samples_split=10,
                subsample=0.8,
                random_state=42
            )
        }
        
        best_model = None
        best_mae = float('inf')
        best_name = None
        
        logger.info(f"\n  Testing multiple algorithms:")
        
        for name, model in models_to_test.items():
            # Train
            model.fit(X_train, y_train)
            
            # Validate
            y_pred = model.predict(X_val)
            mae = mean_absolute_error(y_val, y_pred)
            rmse = np.sqrt(mean_squared_error(y_val, y_pred))
            r2 = r2_score(y_val, y_pred)
            
            # MAPE (only for prices > 10)
            mask = y_val > 10
            mape = np.mean(np.abs((y_val[mask] - y_pred[mask]) / y_val[mask])) * 100 if mask.sum() > 0 else 0
            
            logger.info(f"    {name:20s} MAE: {mae:6.2f} | RMSE: {rmse:6.2f} | R²: {r2:6.4f} | MAPE: {mape:6.2f}%")
            
            # Track best
            if mae < best_mae:
                best_mae = mae
                best_model = model
                best_name = name
        
        logger.info(f"\n  ✓ Selected: {best_name} (MAE: {best_mae:.2f} EUR/MWh)")
        
        # Log feature importance for best model
        if hasattr(best_model, 'feature_importances_'):
            logger.info(f"\n  Top 10 features:")
            importance_df = pd.DataFrame({
                'feature': self.feature_columns,
                'importance': best_model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            for idx, row in importance_df.head(10).iterrows():
                logger.info(f"    {row['feature']:30s} {row['importance']:.4f}")
        
        return best_model