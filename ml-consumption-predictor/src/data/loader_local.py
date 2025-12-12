"""
Data loader for local CSV files
Loads from ml-consumption-predictor/data/csvs/
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict, Optional, Tuple
import logging
import os

from src.config.settings import settings
from src.utils.spark_utils import get_spark
from src.utils.data_validator import DataValidator

logger = logging.getLogger(__name__)


class LocalDataLoader:
    """Loads data from local CSV files in data/csvs/"""
    
    def __init__(self):
        self.spark = get_spark()
        self.paths = settings.paths
        
        # Log the paths being used
        logger.info(f"Local data root: {self.paths.data_root}")
        logger.info(f"Consumption path: {self.paths.consumption_path}")
        logger.info(f"Weather path: {self.paths.weather_path}")
        logger.info(f"Forecast path: {self.paths.forecast_path}")
        logger.info(f"Analytics path: {self.paths.analytics_path}")
    
    def load_historical_consumption(self, years: List[int]) -> DataFrame:
        """
        Load historical consumption data from CSV files
        
        NEW Schema: datetime, municipalityCode, consumptionKwh, timeDK, municipality, regionName, dkArea
        
        Args:
            years: List of years to load
        
        Returns:
            Spark DataFrame with consumption data (with timeDK column for compatibility)
        """
        logger.info(f"Loading consumption data for years: {years}")
        
        dfs = []
        for year in years:
            path = self.paths.get_consumption_path(year)
            
            if not os.path.exists(path):
                logger.warning(f"File not found: {path}")
                continue
            
            try:
                df = self.spark.read.csv(path, header=True, inferSchema=True)
                
                # Parse timestamps - new schema has both datetime and timeDK
                if "datetime" in df.columns:
                    df = df.withColumn("datetime", F.to_timestamp("datetime"))
                
                if "timeDK" in df.columns:
                    df = df.withColumn("timeDK", F.to_timestamp("timeDK"))
                elif "datetime" in df.columns:
                    # If only datetime exists, create timeDK from it
                    df = df.withColumn("timeDK", F.col("datetime"))
                
                # Ensure consumptionKwh column exists (capital K in new schema)
                if "consumptionKwh" in df.columns and "consumptionKwh" not in df.columns:
                    df = df.withColumnRenamed("consumptionKwh", "consumptionKwh")
                
                dfs.append(df)
                logger.debug(f"Loaded {path}: {df.count()} records")
            except Exception as e:
                logger.warning(f"Error loading {path}: {e}")
        
        if not dfs:
            raise ValueError(f"No consumption data found for years {years}")
        
        # Union all dataframes
        result = dfs[0]
        for df in dfs[1:]:
            result = result.unionByName(df, allowMissingColumns=True)
        
        logger.info(f"Loaded {result.count():,} consumption records")
        return result
    
    def load_historical_weather(
        self,
        years: List[int],
        parameter: str
    ) -> DataFrame:
        """
        Load historical weather data from CSV files
        
        Schema for temp: timeObserved, stationId, stationName, mean_temp, lon, lat, dkArea, municipalityCode
        Schema for sun: timeObserved, stationId, stationName, mean_radiation, lon, lat, dkArea, municipalityCode
        Schema for wind: timeObserved, stationId, stationName, mean_wind_speed, lon, lat, dkArea, municipalityCode
        
        Args:
            years: List of years to load
            parameter: Weather parameter (temperature-2m, direct-solar-exposure, wind-speed-10m)
        
        Returns:
            Spark DataFrame with weather data
        """
        logger.info(f"Loading {parameter} data for years: {years}")
        
        # Map parameter to value column
        value_column_map = {
            'temperature-2m': 'mean_temp',
            'direct-solar-exposure': 'mean_radiation',
            'wind-speed-10m': 'mean_wind_speed'
        }
        value_col = value_column_map.get(parameter, 'value')
        
        dfs = []
        for year in years:
            path = self.paths.get_weather_path(parameter, year)
            
            if not os.path.exists(path):
                logger.warning(f"File not found: {path}")
                continue
            
            try:
                df = self.spark.read.csv(path, header=True, inferSchema=True)
                
                # Parse timestamp (timeObserved)
                df = df.withColumn("timestamp", F.to_timestamp("timeObserved"))
                
                # Rename value column to standard 'value' for processing
                if value_col in df.columns:
                    df = df.withColumn("value", F.col(value_col))
                
                # Add parameter column if not present
                if "parameter" not in df.columns:
                    df = df.withColumn("parameter", F.lit(parameter))
                
                dfs.append(df)
                logger.debug(f"Loaded {path}: {df.count()} records")
            except Exception as e:
                logger.warning(f"Error loading {path}: {e}")
        
        if not dfs:
            raise ValueError(f"No weather data found for {parameter}")
        
        # Union all dataframes
        result = dfs[0]
        for df in dfs[1:]:
            result = result.unionByName(df, allowMissingColumns=True)
        
        logger.info(f"Loaded {result.count():,} weather records for {parameter}")
        return result
    
    def load_forecast_weather(
        self, 
        parameter: str,
        specific_dates: Optional[List[Tuple[int, int, int]]] = None
    ) -> DataFrame:
        """
        Load forecast weather data from CSV file
        
        Schema same as historical weather
        
        Args:
            parameter: Weather parameter
            specific_dates: Optional list of (year, month, day) tuples for filtering
        
        Returns:
            Spark DataFrame with forecast data
        """
        path = self.paths.get_forecast_path(parameter)
        
        logger.info(f"Loading forecast {parameter} from {path}")
        
        if not os.path.exists(path):
            raise FileNotFoundError(f"Forecast file not found: {path}")
        
        # Map parameter to value column
        value_column_map = {
            'temperature-2m': 'mean_temp',
            'direct-solar-exposure': 'mean_radiation',
            'wind-speed-10m': 'mean_wind_speed'
        }
        value_col = value_column_map.get(parameter, 'value')
        
        try:
            df = self.spark.read.csv(path, header=True, inferSchema=True)
            
            # Parse timestamp (timeObserved)
            df = df.withColumn("timestamp", F.to_timestamp("timeObserved"))
            
            # Rename value column to standard 'value' for processing
            if value_col in df.columns:
                df = df.withColumn("value", F.col(value_col))
            
            # Add parameter column if not present
            if "parameter" not in df.columns:
                df = df.withColumn("parameter", F.lit(parameter))
            
            # If specific dates requested, filter to those dates
            if specific_dates:
                logger.info(f"Filtering forecast to {len(specific_dates)} specific dates")
                
                # Create filter condition for specific dates
                date_conditions = []
                for year, month, day in specific_dates:
                    condition = (
                        (F.year("timestamp") == year) &
                        (F.month("timestamp") == month) &
                        (F.dayofmonth("timestamp") == day)
                    )
                    date_conditions.append(condition)
                
                # Combine with OR
                combined_condition = date_conditions[0]
                for condition in date_conditions[1:]:
                    combined_condition = combined_condition | condition
                
                df = df.filter(combined_condition)
            
            logger.info(f"Loaded {df.count():,} forecast records for {parameter}")
            return df
        except Exception as e:
            logger.error(f"Error loading forecast from {path}: {e}")
            raise
    
    def load_complete_training_data(
        self,
        requested_years: int,
        prediction_year: int
    ) -> Dict[str, DataFrame]:
        """
        Load complete training data with validation
        
        IMPORTANT: Loads historical years EXCLUDING current year for training,
        then adds current year for trend calculation.
        
        For example, if current year is 2024 and requested_years=2:
        - Loads 2022, 2023 for model training
        - Adds 2024 for year-over-year trend calculation
        
        Args:
            requested_years: Number of historical years to load (excluding current year)
            prediction_year: The year we're making predictions for
        
        Returns:
            Dictionary with 'consumption', 'temp', 'sun', 'wind' DataFrames
        """
        
        # Training years: exclude current year
        # For requested_years=2 in 2024: [2022, 2023]
        training_years = list(range(prediction_year - requested_years, prediction_year))
        
        # All years including current for trend calculation
        # For requested_years=2 in 2024: [2022, 2023, 2024]
        all_years = list(range(prediction_year - requested_years, prediction_year + 1))
        
        logger.info(f"Attempting to load {requested_years} training years: {training_years}")
        logger.info(f"Plus current year {prediction_year} for trend calculation")
        logger.info(f"Total years to load: {all_years}")
        
        # Load all data (including current year for trends)
        consumption_df = self.load_historical_consumption(all_years)
        temp_df = self.load_historical_weather(all_years, 'temperature-2m')
        sun_df = self.load_historical_weather(all_years, 'direct-solar-exposure')
        
        # Wind is optional
        try:
            wind_df = self.load_historical_weather(all_years, 'wind-speed-10m')
        except Exception as e:
            logger.warning(f"Wind data not available: {e}")
            wind_df = None
        
        # Validate and find common date range
        weather_dfs = {'temp': temp_df, 'sun': sun_df}
        if wind_df is not None:
            weather_dfs['wind'] = wind_df
        
        valid_start, valid_end, actual_years = DataValidator.find_valid_date_range(
            consumption_df,
            weather_dfs,
            requested_years + 1  # +1 because we loaded current year too
        )
        
        # Filter to valid range
        consumption_df = DataValidator.filter_by_date_range(
            consumption_df, 'timeDK', valid_start, valid_end
        )
        temp_df = DataValidator.filter_by_date_range(
            temp_df, 'timestamp', valid_start, valid_end
        )
        sun_df = DataValidator.filter_by_date_range(
            sun_df, 'timestamp', valid_start, valid_end
        )
        
        if wind_df is not None:
            wind_df = DataValidator.filter_by_date_range(
                wind_df, 'timestamp', valid_start, valid_end
            )
        
        return {
            'consumption': consumption_df,
            'temp': temp_df,
            'sun': sun_df,
            'wind': wind_df
        }