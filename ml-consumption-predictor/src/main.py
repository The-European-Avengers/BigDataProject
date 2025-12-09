"""
Main Spark Job entry point for Energy Consumption ML Predictor
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
        description='Energy Consumption ML Predictor - Spark Job'
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
        logger.info("ENERGY CONSUMPTION ML PREDICTOR - SPARK JOB")
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
        
        training_data = loader.load_complete_training_data(training_years)
        
        logger.info("✓ Training data loaded successfully\n")
        
        # STEP 2: Train model
        logger.info("=" * 80)
        logger.info("STEP 2: Training Model")
        logger.info("=" * 80)
        
        trainer = ModelTrainer()
        model, trend_multipliers = trainer.train(training_data)
        
        logger.info("✓ Model trained successfully\n")
        
        # STEP 3: Load forecast data
        logger.info("=" * 80)
        logger.info("STEP 3: Loading Forecast Data")
        logger.info("=" * 80)
        
        # Pass specific_dates to loader for smart forecast loading
        temp_forecast = loader.load_forecast_weather(
            'temperature-2m',
            specific_dates=prediction_dates  # Will trigger special logic in K8s mode
        )
        sun_forecast = loader.load_forecast_weather(
            'direct-solar-exposure',
            specific_dates=prediction_dates
        )
        
        logger.info("✓ Forecast data loaded successfully\n")
        
        # STEP 4: Generate predictions
        logger.info("=" * 80)
        logger.info("STEP 4: Generating Predictions")
        logger.info("=" * 80)
        
        predictor = EnergyPredictor(
            model,
            trend_multipliers,
            training_data['consumption']
        )
        
        predictions_df = predictor.predict(temp_forecast, sun_forecast)
        
        logger.info("✓ Predictions generated successfully\n")
        
        # STEP 5: Write predictions for each day
        logger.info("=" * 80)
        logger.info("STEP 5: Writing Predictions")
        logger.info("=" * 80)
        
        for year, month, day in final_prediction_dates:
            logger.info(f"Writing predictions for {year}-{month:02d}-{day:02d}...")
            
            # Filter predictions for this day
            day_predictions = predictions_df.filter(
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