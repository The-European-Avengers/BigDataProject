"""
Data writer for Kubernetes Parquet files
Handles HDFS structure with analytics output
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging
import uuid

from src.config.settings import settings
from src.utils.spark_utils import get_spark

logger = logging.getLogger(__name__)


class K8sDataWriter:
    """Writes predictions to Kubernetes Parquet files"""
    
    def __init__(self):
        self.spark = get_spark()
        self.paths = settings.paths
    
    def write_predictions(
        self,
        predictions_df: DataFrame,
        year: int,
        month: int,
        day: int
    ):
        """
        Write predictions to Kubernetes storage
        
        Strategy:
        1. Write to analytics: /analytics/<year>-<month>-<day>.parquet
        2. Archive with UUID: /historical/archives/<year>/<month>/analytics/<uuid>.parquet
        
        Columns: timestamp, municipalityCode, consumptionkWh, mean_temp, mean_radiation,
                 mean_wind_speed, productionkWh, price
        
        Args:
            predictions_df: DataFrame with predictions
            year: Year of prediction
            month: Month of prediction
            day: Day of prediction
        """
        analytics_path = self.paths.get_analytics_path(year, month, day)
        
        # Generate UUID for archive
        prediction_uuid = str(uuid.uuid4())[:8]
        archive_path = self.paths.get_archive_analytics_path(year, month, prediction_uuid)
        
        # Select columns for output
        output_df = predictions_df.select(
            F.col("timestamp"),
            F.col("municipalityCode"),
            F.col("consumptionkWh"),
            F.col("mean_temp"),
            F.col("mean_radiation"),
            F.col("mean_wind_speed"),
            F.col("productionkWh"),
            F.col("price")
        )
        
        # Step 1: Write to analytics (current predictions)
        logger.info(f"Writing current predictions to {analytics_path}")
        try:
            output_df.write \
                .mode("overwrite") \
                .parquet(analytics_path)
            
            count = output_df.count()
            logger.info(f"✓ Written {count:,} predictions to analytics")
        except Exception as e:
            logger.error(f"Failed to write analytics: {e}")
            raise
        
        # Step 2: Archive predictions with UUID
        logger.info(f"Archiving predictions to {archive_path}")
        try:
            output_df.write \
                .mode("overwrite") \
                .parquet(archive_path)
            
            logger.info(f"✓ Archived predictions with UUID: {prediction_uuid}")
        except Exception as e:
            logger.error(f"Failed to write archive: {e}")
            # Don't fail if archive fails, analytics write is more important
            logger.warning("Continuing despite archive failure...")