"""
Data writer for Kubernetes Parquet files
Handles HDFS structure with analytics output
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging
from datetime import datetime
from typing import Set

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
        Write predictions for a single day to both archive and main analytics
        
        This method is called per day, but we need to aggregate all days
        and write once at the end. Use write_all_predictions instead.
        
        This method is kept for compatibility but logs a warning.
        """
        logger.warning(
            "write_predictions() called for single day. "
            "Use write_all_predictions() to write all predictions at once."
        )
        # Still write archive for this day
        self._write_archive(predictions_df, year, month)
    
    def write_all_predictions(
        self,
        predictions_df: DataFrame
    ):
        """
        Write all predictions to both archive (by month) and main analytics
        
        Strategy:
        1. Delete current /analytics/predictions.parquet
        2. Write new /analytics/predictions.parquet with ALL predictions
        3. Archive predictions by month to /historical/archives/<year>/<month>/analytics/predictions_<timestamp>.parquet
        
        Output columns: timestamp, municipalityCode, consumptionkWh, mean_temp, 
                       mean_radiation, mean_wind_speed, productionkWh, price
        
        Args:
            predictions_df: DataFrame with ALL predictions (all days)
        """
        logger.info("=" * 80)
        logger.info("WRITING PREDICTIONS TO HDFS")
        logger.info("=" * 80)
        
        # Select columns for output
        output_df = predictions_df.select(
            F.col("timestamp"), 
            F.col("municipalityCode").cast("int"),
            F.col("consumptionkWh").cast("double"),
            F.col("mean_temp").cast("double"),
            F.col("mean_radiation").cast("double"),
            F.col("mean_wind_speed").cast("double"),
            F.col("productionkWh").cast("double"),
            F.col("price").cast("double")
        )
        
        count = output_df.count()
        logger.info(f"Total predictions to write: {count:,}")
        
        # Get unique months in predictions
        months_in_data = output_df.select(
            F.year("timestamp").alias("year"),
            F.month("timestamp").alias("month")
        ).distinct().collect()
        
        year_months = [(row.year, row.month) for row in months_in_data]
        year_months.sort()
        
        logger.info(f"Predictions span {len(year_months)} month(s): {year_months}")
        
        # Step 1: Write to main analytics (overwrite)
        analytics_path = f"{self.paths.base_path}/analytics/predictions.parquet"
        logger.info(f"\nStep 1: Writing main analytics to {analytics_path}")
        
        try:
            # Delete existing file first
            try:
                self._delete_hdfs_path(analytics_path)
                logger.info("  ✓ Deleted old predictions.parquet")
            except Exception as e:
                logger.debug(f"  No existing file to delete: {e}")
            
            # Write new predictions
            output_df.write \
                .mode("overwrite") \
                .parquet(analytics_path)
            
            logger.info(f"  ✓ Written {count:,} predictions to main analytics")
            
        except Exception as e:
            logger.error(f"  ✗ Failed to write main analytics: {e}")
            raise
        
        # Step 2: Archive by month
        logger.info(f"\nStep 2: Archiving predictions by month")

        current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for year, month in year_months:
            logger.info(f"  Archiving {year}-{month:02d}...")
            
            # Filter to this month
            month_df = output_df.filter(
                (F.year("timestamp") == year) &
                (F.month("timestamp") == month)
            )
            
            month_count = month_df.count()
            
            if month_count == 0:
                logger.warning(f"    No data for {year}-{month:02d}, skipping")
                continue
            
            # Archive path with current timestamp
            archive_path = f"{self.paths.base_path}/historical/archives/{year}/{month:02d}/analytics/predictions_{current_timestamp}.parquet"
            
            try:
                # CRITICAL: Use overwrite mode with full path
                month_df.coalesce(1).write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(archive_path)
                
                logger.info(f"    ✓ Archived {month_count:,} predictions to {archive_path}")
                
            except Exception as e:
                logger.error(f"    ✗ Failed to archive {year}-{month:02d}: {e}", exc_info=True)
                # Don't fail the entire job if archive fails
                logger.warning("    Continuing despite archive failure...")
        
        logger.info("\n" + "=" * 80)
        logger.info("PREDICTIONS WRITTEN SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Main: /analytics/predictions.parquet ({count:,} records)")
        logger.info(f"Archives: {len(year_months)} month(s) in /historical/archives/")
        logger.info("=" * 80)
    
    def _write_archive(
        self,
        predictions_df: DataFrame,
        year: int,
        month: int
    ):
        """
        Write archive for specific month
        (Used internally or for single-day writes)
        """
        current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_path = f"{self.paths.base_path}/historical/archives/{year}/{month:02d}/analytics/predictions_{current_timestamp}.parquet"
        
        logger.info(f"Archiving to {archive_path}")
        
        try:
            # Select columns
            output_df = predictions_df.select(
                F.col("timestamp"), 
                F.col("municipalityCode").cast("int"),
                F.col("consumptionkWh").cast("double"),
                F.col("mean_temp").cast("double"),
                F.col("mean_radiation").cast("double"),
                F.col("mean_wind_speed").cast("double"),
                F.col("productionkWh").cast("double"),
                F.col("price").cast("double")
            )
            
            output_df.write \
                .mode("overwrite") \
                .parquet(archive_path)
            
            count = output_df.count()
            logger.info(f"  ✓ Archived {count:,} predictions")
            
        except Exception as e:
            logger.error(f"  ✗ Failed to archive: {e}")
            # Don't fail job
            logger.warning("  Continuing despite archive failure...")
    
    def _delete_hdfs_path(self, path: str):
        """
        Delete HDFS path using Hadoop FileSystem API
        
        Args:
            path: HDFS path to delete
        """
        try:
            # Use Hadoop FileSystem API through py4j
            sc = self.spark.sparkContext
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                sc._jvm.java.net.URI.create(path),
                sc._jsc.hadoopConfiguration()
            )
            hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
            
            if fs.exists(hadoop_path):
                fs.delete(hadoop_path, True)  # True = recursive
                logger.debug(f"Deleted {path}")
            else:
                logger.debug(f"Path does not exist: {path}")
                
        except Exception as e:
            logger.warning(f"Could not delete {path}: {e}")
            # Re-raise to let caller handle
            raise