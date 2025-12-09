"""
Data writer for local CSV files
Writes to ml-consumption-predictor/data/analytics/
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

from src.config.settings import settings

logger = logging.getLogger(__name__)


class LocalDataWriter:
    """Writes predictions to local CSV files in data/analytics/"""
    
    def __init__(self):
        self.paths = settings.paths
        
        # Log the analytics path being used
        logger.info(f"Analytics output path: {self.paths.analytics_path}")
    
    def write_predictions(
        self,
        predictions_df: DataFrame,
        year: int,
        month: int,
        day: int
    ):
        """
        Write predictions to local CSV file
        
        Path: data/analytics/<year>-<month>-<day>.csv
        
        Columns: timestamp, municipalityCode, consumptionkWh, mean_temp, mean_radiation,
                 mean_wind_speed, productionkWh, price
        
        Args:
            predictions_df: DataFrame with predictions
            year: Year of prediction
            month: Month of prediction
            day: Day of prediction
        """
        output_path = self.paths.get_analytics_path(year, month, day)
        
        logger.info(f"Writing predictions to {output_path}")
        
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
        
        # Convert to pandas for single CSV file
        output_pd = output_df.toPandas()
        
        # Write to CSV
        output_pd.to_csv(output_path, index=False)
        
        logger.info(f"✓ Written {len(output_pd):,} predictions to {output_path}")