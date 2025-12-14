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
        
        Output Schema:
        - timestamp: datetime
        - municipalityCode: int
        - dkArea: int
        - consumptionkWh: float
        - mean_temp: float
        - mean_radiation: float
        - mean_wind_speed: float
        - productionkWh: float (from production calculation)
        - price: float (EUR/MWh, same for all municipalities in same dkArea)
        
        Args:
            predictions_df: DataFrame with predictions
            year: Year of prediction
            month: Month of prediction
            day: Day of prediction
        """
        output_path = self.paths.get_analytics_path(year, month, day)
        
        logger.info(f"Writing predictions to {output_path}")
        
        # Select and order columns for output
        output_df = predictions_df.select(
            F.col("timestamp"),
            F.col("municipalityCode"),
            F.col("dkArea"),
            F.col("consumptionkWh"),
            F.col("mean_temp"),
            F.col("mean_radiation"),
            F.col("mean_wind_speed"),
            F.col("productionkWh"),
            F.col("price")
        )
        
        # Convert to pandas for single CSV file
        output_pd = output_df.toPandas()
        
        # Sort by timestamp and municipalityCode for readability
        output_pd = output_pd.sort_values(['timestamp', 'municipalityCode'])
        
        # Write to CSV
        output_pd.to_csv(output_path, index=False)
        
        logger.info(f"✓ Written {len(output_pd):,} predictions to {output_path}")
        
        # Log summary statistics
        logger.info(f"  Consumption range: {output_pd['consumptionkWh'].min():.2f} - {output_pd['consumptionkWh'].max():.2f} kWh")
        logger.info(f"  Production range: {output_pd['productionkWh'].min():.2f} - {output_pd['productionkWh'].max():.2f} kWh")
        logger.info(f"  Price range: {output_pd['price'].min():.2f} - {output_pd['price'].max():.2f} EUR/MWh")