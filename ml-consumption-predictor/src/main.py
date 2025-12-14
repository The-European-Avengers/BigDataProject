"""
Main Spark Job entry point for Energy Consumption ML Predictor
Now includes price prediction based on consumption and production
"""

import logging
import sys
from datetime import datetime
from typing import List, Tuple, Optional
import argparse

from pyspark.sql import functions as F

from src.config.settings import initialize_settings, settings, DeploymentMode
from src.utils.spark_utils import spark_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Energy Consumption & Price ML Predictor - Spark Job'
    )
    
    parser.add_argument(
        '--days',
        type=str,
        default=None,
        help='Comma-separated list of dates to predict (YYYY-MM-DD format). '
             'If not specified, predicts all days in forecast data.'
    )
    
    parser.add_argument(
        '--training-years',
        type=int,
        default=4,
        help='Number of years for training data (default: 4)'
    )
    
    parser.add_argument(
        '--mode',
        type=str,
        choices=['local', 'kubernetes'],
        default='kubernetes',
        help='Deployment mode (default: kubernetes)'
    )
    
    return parser.parse_args()


def parse_date_list(days_str: str) -> List[Tuple[int, int, int]]:
    """
    Parse comma-separated date list
    
    Args:
        days_str: Comma-separated dates in YYYY-MM-DD format
    
    Returns:
        List of (year, month, day) tuples
    """
    dates = []
    for date_str in days_str.split(','):
        date_str = date_str.strip()
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            dates.append((dt.year, dt.month, dt.day))
        except ValueError:
            logger.error(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")
            raise
    
    return dates


def get_forecast_dates(loader, specific_dates: Optional[List[Tuple[int, int, int]]]) -> List[Tuple[int, int, int]]:
    """
    Get forecast dates from data
    
    Args:
        loader: Data loader instance
        specific_dates: Optional list of specific dates requested
    
    Returns:
        List of (year, month, day) tuples
    """
    if specific_dates is not None:
        # User specified dates - return those
        return specific_dates
    
    # No specific dates - extract from forecast data
    # Import here to avoid circular dependency
    from src.data.loader_k8s import K8sDataLoader
    
    if isinstance(loader, K8sDataLoader):
        # K8s loader has method to extract dates from forecast
        return loader.get_forecast_dates()
    else:
        # Local loader - extract dates from forecast file
        temp_forecast = loader.load_forecast_weather('temperature-2m')
        
        dates = temp_forecast.select(
            F.year("timestamp").alias("year"),
            F.month("timestamp").alias("month"),
            F.dayofmonth("timestamp").alias("day")
        ).distinct().collect()
        
        date_list = [(row.year, row.month, row.day) for row in dates]
        date_list.sort()
        
        return date_list


def run_spark_job(
    prediction_dates: Optional[List[Tuple[int, int, int]]],
    training_years: int,
    mode: DeploymentMode
):
    """
    Main Spark job execution
    
    Args:
        prediction_dates: List of (year, month, day) tuples to predict.
                         If None, predict all dates in forecast.
        training_years: Number of years for training
        mode: Deployment mode
    """
    try:
        logger.info("=" * 80)
        logger.info("ENERGY CONSUMPTION & PRICE ML PREDICTOR - SPARK JOB")
        logger.info("=" * 80)
        logger.info(f"Mode: {mode.value}")
        logger.info(f"Training years: {training_years}")
        
        # Initialize settings FIRST (before any other imports that use settings)
        initialize_settings(mode)
        logger.info("✓ Settings initialized")
        
        # Now import loaders and writers (they depend on settings being initialized)
        from src.data.loader_local import LocalDataLoader
        from src.data.loader_k8s import K8sDataLoader
        from src.data.writer_local import LocalDataWriter
        from src.data.writer_k8s import K8sDataWriter
        from src.models.trainer import ModelTrainer
        from src.models.predictor import EnergyPredictor
        from src.models.price_trainer import PriceModelTrainer
        from src.models.price_predictor import PricePredictor
        from src.production.calculator import ProductionCalculator
        
        # Initialize loader and writer based on mode
        if mode == DeploymentMode.LOCAL:
            loader = LocalDataLoader()
            writer = LocalDataWriter()
            logger.info("Using local CSV files")
        else:
            loader = K8sDataLoader()
            writer = K8sDataWriter()
            logger.info("Using Kubernetes Avro/Parquet files")
        
        # Determine prediction dates
        final_prediction_dates = get_forecast_dates(loader, prediction_dates)
        
        if prediction_dates is not None:
            logger.info(f"Using specified dates: {len(final_prediction_dates)} days")
        else:
            logger.info(f"Auto-detected {len(final_prediction_dates)} days from forecast data")
        
        logger.info(f"Will generate predictions for {len(final_prediction_dates)} days:")
        for year, month, day in final_prediction_dates[:5]:
            logger.info(f"  - {year}-{month:02d}-{day:02d}")
        if len(final_prediction_dates) > 5:
            logger.info(f"  ... and {len(final_prediction_dates) - 5} more")
        
        # STEP 1: Load training data
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: Loading Training Data")
        logger.info("=" * 80)
        
        training_data = loader.load_complete_training_data(training_years, final_prediction_dates[0][0])
        
        logger.info("✓ Training data loaded successfully\n")
        
        # STEP 2: Train consumption model
        logger.info("=" * 80)
        logger.info("STEP 2: Training Consumption Model")
        logger.info("=" * 80)
        
        consumption_trainer = ModelTrainer()
        consumption_models, trend_multipliers = consumption_trainer.train(training_data)
        
        logger.info("✓ Consumption model trained successfully\n")
        
        # STEP 3: Train price models
        logger.info("=" * 80)
        logger.info("STEP 3: Training Price Models")
        logger.info("=" * 80)
        
        price_trainer = PriceModelTrainer()
        price_models = price_trainer.train(
            training_data['consumption'],
            training_data['production'],
            training_data['price']
        )
        
        logger.info("✓ Price models trained successfully\n")
        
        # STEP 4: Load forecast data
        logger.info("=" * 80)
        logger.info("STEP 4: Loading Forecast Weather Data")
        logger.info("=" * 80)
        
        # Pass specific_dates to loader for smart forecast loading
        temp_forecast = loader.load_forecast_weather(
            'temperature-2m',
            specific_dates=prediction_dates
        )
        sun_forecast = loader.load_forecast_weather(
            'direct-solar-exposure',
            specific_dates=prediction_dates
        )
        
        # Wind is optional
        wind_forecast = None
        try:
            wind_forecast = loader.load_forecast_weather(
                'wind-speed-10m',
                specific_dates=prediction_dates
            )
            logger.info("✓ Wind forecast loaded")
        except Exception as e:
            logger.warning(f"Wind forecast not available: {e}")
        
        logger.info("✓ Forecast data loaded successfully\n")
        
        # STEP 5: Generate consumption predictions
        logger.info("=" * 80)
        logger.info("STEP 5: Generating Consumption Predictions")
        logger.info("=" * 80)
        
        consumption_predictor = EnergyPredictor(
            consumption_models,
            trend_multipliers,
            training_data['consumption']
        )
        
        # Pass wind_forecast to predictor
        consumption_predictions = consumption_predictor.predict(
            temp_forecast, 
            sun_forecast,
            wind_forecast
        )
        
        logger.info("✓ Consumption predictions generated successfully\n")
        
        # STEP 6: Calculate production from forecast
        logger.info("=" * 80)
        logger.info("STEP 6: Calculating Production from Forecast")
        logger.info("=" * 80)
        
        prod_calculator = ProductionCalculator(spark_manager.get_spark_session())
        production_predictions = prod_calculator.calculate_production(
            temp_forecast,
            sun_forecast,
            wind_forecast
        )
        
        logger.info("✓ Production calculated successfully\n")
        
        # STEP 7: Generate price predictions
        logger.info("=" * 80)
        logger.info("STEP 7: Generating Price Predictions")
        logger.info("=" * 80)
        
        price_predictor = PricePredictor(price_models)
        
        # Pass historical prices for constraint calculation
        historical_prices = training_data.get('price', None)
        
        price_predictions = price_predictor.predict(
            consumption_predictions,
            production_predictions,
            historical_prices  # For calculating min/max constraints
        )
        
        logger.info("✓ Price predictions generated successfully\n")
        
        # STEP 8: Merge consumption, production, and price predictions
        logger.info("=" * 80)
        logger.info("STEP 8: Merging Predictions")
        logger.info("=" * 80)
        
        final_predictions = merge_predictions(
            consumption_predictions,
            production_predictions,
            price_predictions
        )
        
        logger.info("✓ Predictions merged successfully\n")
        
        # STEP 9: Write predictions
        logger.info("=" * 80)
        logger.info("STEP 9: Writing Predictions")
        logger.info("=" * 80)
        
        # FIX: For K8s mode, write all predictions at once
        if mode == DeploymentMode.KUBERNETES:
            logger.info("Writing all predictions to HDFS (main + archives)...")
            writer.write_all_predictions(final_predictions)
        else:
            # Local mode: write per day as before
            for year, month, day in final_prediction_dates:
                logger.info(f"Writing predictions for {year}-{month:02d}-{day:02d}...")
                
                # Filter predictions for this day
                day_predictions = final_predictions.filter(
                    (F.year("timestamp") == year) &
                    (F.month("timestamp") == month) &
                    (F.dayofmonth("timestamp") == day)
                )
                
                count = day_predictions.count()
                if count > 0:
                    writer.write_predictions(day_predictions, year, month, day)
                    logger.info(f"  ✓ Wrote {count:,} predictions")
                else:
                    logger.warning(f"  ⚠ No predictions found for {year}-{month:02d}-{day:02d}")
        
        logger.info("\n✓ All predictions written successfully\n")
        
        logger.info("=" * 80)
        logger.info("SPARK JOB COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("SPARK JOB FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}", exc_info=True)
        return 1
    
    finally:
        # Clean up Spark session
        logger.info("\nCleaning up resources...")
        spark_manager.stop()


def merge_predictions(
    consumption_df,
    production_df,
    price_df
):
    """
    Merge consumption, production, and price predictions
    
    Args:
        consumption_df: Consumption predictions (timestamp, municipalityCode, consumptionkWh, 
                                                mean_temp, mean_radiation, mean_wind_speed)
        production_df: Production calculations (timeObserved, municipalityCode, 
                                               windProductionKwh, sunProductionKwh, productionKwh)
        price_df: Price predictions (timestamp, dkArea, price)
    
    Returns:
        Merged DataFrame with all predictions
    """
    logger.info("Merging consumption, production, and price predictions...")
    
    # DEBUG: Check data before merge
    logger.info("\nDEBUG: Checking data before merge...")
    
    cons_count = consumption_df.count()
    prod_count = production_df.count()
    price_count = price_df.count()
    
    logger.info(f"Consumption records: {cons_count:,}")
    logger.info(f"Production records: {prod_count:,}")
    logger.info(f"Price records: {price_count:,}")
    
    # Show sample of consumption predictions
    logger.info("\nConsumption predictions sample (timestamp, municipalityCode):")
    consumption_df.select("timestamp", "municipalityCode").show(5, truncate=False)
    
    # Show sample of production predictions
    logger.info("\nProduction predictions sample (timeObserved, municipalityCode):")
    production_df.select("timeObserved", "municipalityCode", "productionKwh").show(5, truncate=False)
    
    # Drop old placeholder columns from consumption predictions (if they exist)
    for col in ["productionkWh", "price"]:
        if col in consumption_df.columns:
            consumption_df = consumption_df.drop(col)
    
    # Ensure dkArea in consumption
    if "dkArea" not in consumption_df.columns:
        consumption_df = consumption_df.withColumn(
            "dkArea",
            F.when(F.col("municipalityCode") > 400, 1).otherwise(2)
        )
    
    # Rename timeObserved to timestamp in production for join
    production_df = production_df.withColumnRenamed("timeObserved", "timestamp")
    
    # DEBUG: Check if timestamps match
    cons_time_range = consumption_df.select(
        F.min("timestamp").alias("min_ts"),
        F.max("timestamp").alias("max_ts")
    ).collect()[0]
    
    prod_time_range = production_df.select(
        F.min("timestamp").alias("min_ts"),
        F.max("timestamp").alias("max_ts")
    ).collect()[0]
    
    logger.info(f"\nConsumption time range: {cons_time_range.min_ts} to {cons_time_range.max_ts}")
    logger.info(f"Production time range: {prod_time_range.min_ts} to {prod_time_range.max_ts}")
    
    # Check municipality codes overlap
    cons_munis = set([row.municipalityCode for row in 
                      consumption_df.select("municipalityCode").distinct().collect()])
    prod_munis = set([row.municipalityCode for row in 
                      production_df.select("municipalityCode").distinct().collect()])
    
    overlap_munis = cons_munis.intersection(prod_munis)
    logger.info(f"\nConsumption municipalities: {len(cons_munis)}")
    logger.info(f"Production municipalities: {len(prod_munis)}")
    logger.info(f"Overlapping municipalities: {len(overlap_munis)}")
    
    if len(overlap_munis) < len(cons_munis):
        missing_munis = cons_munis - prod_munis
        logger.warning(f"Missing production data for {len(missing_munis)} municipalities: {sorted(list(missing_munis))[:10]}")
    
    # Merge consumption with production
    logger.info("\nMerging consumption with production...")
    merged = consumption_df.join(
        production_df.select(
            "timestamp", 
            "municipalityCode",
            "windProductionKwh",
            "sunProductionKwh", 
            "productionKwh"
        ),
        on=["timestamp", "municipalityCode"],
        how="left"
    )
    
    # Check merge result
    merge_count = merged.count()
    non_null_prod = merged.filter(F.col("productionKwh").isNotNull()).count()
    
    logger.info(f"Merged records: {merge_count:,}")
    logger.info(f"Records with production data: {non_null_prod:,}")
    logger.info(f"Records missing production data: {merge_count - non_null_prod:,}")
    
    # Merge with price (price is per dkArea, so same price for all municipalities in area)
    logger.info("\nMerging with price...")
    merged = merged.join(
        price_df,
        on=["timestamp", "dkArea"],
        how="left"
    )
    
    # Fill nulls
    merged = merged.fillna({
        'windProductionKwh': 0.0,
        'sunProductionKwh': 0.0,
        'productionKwh': 0.0,
        'price': 0.0
    })
    
    # Rename productionKwh to match output schema (lowercase 'k')
    merged = merged.withColumnRenamed("productionKwh", "productionkWh")
    
    # FIX: Remove any duplicate rows (keep first occurrence)
    # This ensures only ONE row per timestamp + municipalityCode
    logger.info("\nRemoving any duplicate rows...")
    before_dedup = merged.count()
    merged = merged.dropDuplicates(["timestamp", "municipalityCode"])
    after_dedup = merged.count()
    
    if before_dedup != after_dedup:
        logger.warning(f"  Removed {before_dedup - after_dedup:,} duplicate rows")
    else:
        logger.info(f"  No duplicates found")
    
    # Final stats
    final_prod_sum = merged.agg(F.sum("productionkWh")).collect()[0][0]
    logger.info(f"\nFinal merged data:")
    logger.info(f"  Total records: {merged.count():,}")
    logger.info(f"  Total production: {final_prod_sum:,.0f} kWh")
    
    return merged


if __name__ == "__main__":
    args = parse_args()
    
    # Parse mode
    mode = DeploymentMode.LOCAL if args.mode == 'local' else DeploymentMode.KUBERNETES
    
    # Parse dates if provided
    prediction_dates = None
    if args.days:
        prediction_dates = parse_date_list(args.days)
        logger.info(f"Parsed {len(prediction_dates)} specific dates from command line")
    
    # Run job
    exit_code = run_spark_job(
        prediction_dates=prediction_dates,
        training_years=args.training_years,
        mode=mode
    )
    
    sys.exit(exit_code)