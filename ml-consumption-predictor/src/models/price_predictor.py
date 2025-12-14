"""
Price prediction logic - SIMPLIFIED
No lag features, just time + supply/demand
"""

import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pandas as pd
import logging
import math
from typing import Dict

from src.utils.spark_utils import get_spark

logger = logging.getLogger(__name__)


class PricePredictor:
    """Predicts electricity prices for DK areas"""
    
    def __init__(self, models: Dict[int, any]):
        """
        Initialize predictor
        
        Args:
            models: Dictionary of dkArea -> trained model
        """
        self.models = models
        self.spark = get_spark()
        self.feature_columns = [
            'hour', 'day_of_week', 'month', 'day_of_year', 'is_weekend',
            'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
            'total_consumption', 'total_production', 
            'wind_production', 'solar_production',
            'production_ratio', 'net_demand', 'renewable_percentage',
            'is_peak_hour', 'is_night_hour'
        ]
    
    def predict(
        self,
        consumption_forecast: DataFrame,
        production_forecast: DataFrame,
        historical_prices: DataFrame = None
    ) -> DataFrame:
        """
        Generate price predictions for DK1 and DK2
        
        Args:
            consumption_forecast: Consumption predictions
            production_forecast: Production calculations
            historical_prices: Historical prices for constraint calculation
        
        Returns:
            DataFrame with columns: timestamp, dkArea, price
        """
        logger.info("=" * 80)
        logger.info("PRICE PREDICTION - DK1 and DK2")
        logger.info("=" * 80)
        
        # Calculate historical price constraints if available
        price_constraints = self._calculate_price_constraints(
            consumption_forecast, 
            historical_prices
        )
        
        # Prepare forecast features
        forecast_df = self._prepare_forecast_features(
            consumption_forecast,
            production_forecast
        )
        
        # Predict for DK1 and DK2
        predictions = []
        
        for dk_area in [1, 2]:
            logger.info(f"Predicting prices for DK{dk_area}...")
            area_predictions = self._predict_single_area(
                forecast_df, 
                dk_area, 
                price_constraints
            )
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
        """Prepare forecast features"""
        
        logger.info("Preparing price forecast features...")
        
        # Ensure dkArea exists
        if "dkArea" not in consumption_forecast.columns:
            consumption_forecast = consumption_forecast.withColumn(
                "dkArea",
                F.when(F.col("municipalityCode") > 400, 1).otherwise(2)
            )
        
        # Aggregate consumption
        consumption_agg = consumption_forecast.groupBy("timestamp", "dkArea").agg(
            F.sum("consumptionkWh").alias("total_consumption")
        )
        
        # Aggregate production
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
        merged = self._add_derived_features(merged)
        
        # Fill nulls
        merged = merged.fillna(0.0)
        
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
    
    def _calculate_price_constraints(
        self,
        forecast_df: DataFrame,
        historical_prices: DataFrame
    ) -> dict:
        """
        Calculate min/max price constraints from previous year's same month
        
        Args:
            forecast_df: Forecast data to get prediction dates
            historical_prices: Historical price data
        
        Returns:
            Dictionary with constraints per (dkArea, year, month)
        """
        if historical_prices is None:
            logger.warning("No historical prices provided - using default constraints")
            # Default constraints (typical Danish spot prices)
            return {
                (1, None, None): (0.0, 500.0),  # DK1
                (2, None, None): (0.0, 500.0)   # DK2
            }
        
        logger.info("Calculating price constraints from historical data...")
        
        # Get forecast year/months
        forecast_dates = forecast_df.select(
            F.year("timestamp").alias("year"),
            F.month("timestamp").alias("month")
        ).distinct().collect()
        
        constraints = {}
        
        # For each forecast year/month, get previous year's same month statistics
        for row in forecast_dates:
            pred_year = row.year
            pred_month = row.month
            prev_year = pred_year - 1
            
            # Get historical prices for previous year, same month
            prev_year_month = historical_prices.filter(
                (F.year("timestamp") == prev_year) & 
                (F.month("timestamp") == pred_month)
            )
            
            # Calculate constraints per dkArea
            for dk_area in [1, 2]:
                area_prices = prev_year_month.filter(F.col("dkArea") == dk_area)
                
                # Check if we have data
                if area_prices.count() > 0:
                    stats = area_prices.select(
                        F.min("price_EUR_MWh").alias("min_price"),
                        F.max("price_EUR_MWh").alias("max_price"),
                        F.avg("price_EUR_MWh").alias("avg_price")
                    ).collect()[0]
                    
                    min_price = stats.min_price
                    max_price = stats.max_price
                    avg_price = stats.avg_price
                    
                    # Add some buffer to avoid being too restrictive
                    # Expand range by 20% on each side
                    buffer = 0.2
                    price_range = max_price - min_price
                    min_constraint = max(0.0, min_price - price_range * buffer)
                    max_constraint = max_price + price_range * buffer
                    
                    constraints[(dk_area, pred_year, pred_month)] = (min_constraint, max_constraint)
                    
                    logger.info(f"  DK{dk_area} {pred_year}-{pred_month:02d}: "
                               f"Constraints [{min_constraint:.1f}, {max_constraint:.1f}] EUR/MWh "
                               f"(based on {prev_year}-{pred_month:02d}: avg={avg_price:.1f})")
                else:
                    # No data for previous year - use wider default
                    logger.warning(f"  DK{dk_area} {pred_year}-{pred_month:02d}: "
                                  f"No historical data for {prev_year}-{pred_month:02d}, using defaults")
                    constraints[(dk_area, pred_year, pred_month)] = (0.0, 500.0)
        
        return constraints
    
    def _predict_single_area(
        self,
        forecast_df: DataFrame,
        dk_area: int,
        price_constraints: dict = None
    ) -> pd.DataFrame:
        """Predict prices for single area"""
        
        if dk_area not in self.models:
            raise ValueError(f"No model for DK{dk_area}")
        
        model = self.models[dk_area]
        
        # Filter to single area
        area_df = forecast_df.filter(F.col("dkArea") == dk_area)
        
        # Disable Arrow
        spark = self.spark
        arrow_enabled = spark.conf.get("spark.sql.execution.arrow.pyspark.enabled", "false")
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        
        try:
            area_pd = area_df.select(["timestamp"] + self.feature_columns).toPandas()
        finally:
            spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", arrow_enabled)
        
        if len(area_pd) == 0:
            logger.warning(f"No data for DK{dk_area}")
            return pd.DataFrame(columns=['timestamp', 'dkArea', 'price'])
        
        # Predict
        X_forecast = area_pd[self.feature_columns].apply(pd.to_numeric, errors='coerce').fillna(0)
        predictions = model.predict(X_forecast.values)
        
        # Apply historical constraints
        if price_constraints:
            logger.info(f"  Applying historical price constraints...")
            
            # Extract year and month for each prediction
            years = area_pd['timestamp'].dt.year
            months = area_pd['timestamp'].dt.month
            
            constrained_predictions = []
            clips_applied = 0
            
            for i, (pred, year, month) in enumerate(zip(predictions, years, months)):
                # Try to find constraint for this year/month
                constraint_key = (dk_area, year, month)
                if constraint_key not in price_constraints:
                    # Try without year/month (default)
                    constraint_key = (dk_area, None, None)
                
                if constraint_key in price_constraints:
                    min_price, max_price = price_constraints[constraint_key]
                    
                    # Clip prediction to historical range
                    original_pred = pred
                    pred = max(min_price, min(max_price, pred))
                    
                    if abs(pred - original_pred) > 1.0:  # Significant clipping
                        clips_applied += 1
                    
                    constrained_predictions.append(pred)
                else:
                    # No constraint found, use basic clipping
                    constrained_predictions.append(max(0.0, min(500.0, pred)))
            
            predictions = np.array(constrained_predictions)
            
            if clips_applied > 0:
                logger.info(f"    Clipped {clips_applied}/{len(predictions)} predictions "
                           f"({clips_applied/len(predictions)*100:.1f}%) to historical range")
        else:
            # No constraints, use basic clipping
            predictions = predictions.clip(min=0.0, max=500.0)
        
        # Results
        result_df = pd.DataFrame({
            'timestamp': area_pd['timestamp'],
            'dkArea': dk_area,
            'price': predictions
        })
        
        logger.info(f"  DK{dk_area}: {len(result_df):,} predictions, "
                   f"avg: {result_df['price'].mean():.2f} EUR/MWh, "
                   f"range: {result_df['price'].min():.2f} - {result_df['price'].max():.2f}")
        
        return result_df