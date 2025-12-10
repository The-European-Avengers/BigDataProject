"""
Data loader for Kubernetes Avro files
Handles HDFS structure with streaming forecast batches
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict, Optional, Tuple
import logging

from src.config.settings import settings
from src.utils.spark_utils import get_spark
from src.utils.data_validator import DataValidator

logger = logging.getLogger(__name__)


class K8sDataLoader:
    """Loads data from Kubernetes/HDFS Avro files"""
    
    def __init__(self):
        self.spark = get_spark()
        self.paths = settings.paths
    
    def load_historical_consumption(self, years: List[int]) -> DataFrame:
        """
        Load historical consumption data from Avro files
        
        Schema: consumptionKwh, heatingCategory, housingCategory, municipality,
                municipalityCode, regionName, timeDK, timeUTC, dkArea
        
        Path: /historical/<year>/consumption/<month>.avro/part-*.avro
        
        Args:
            years: List of years to load
        
        Returns:
            Spark DataFrame with consumption data
        """
        logger.info(f"Loading consumption data for years: {years}")
        
        dfs = []
        for year in years:
            for month in range(1, 13):
                path = self.paths.get_consumption_path(year, month)
                
                try:
                    df = self.spark.read.format("avro").load(path)
                    
                    # Parse timestamps
                    df = df.withColumn("timeDK", F.to_timestamp("timeDK"))
                    df = df.withColumn("timeUTC", F.to_timestamp("timeUTC"))
                    
                    dfs.append(df)
                    logger.debug(f"Loaded {path}")
                except Exception as e:
                    logger.debug(f"Could not load {path}: {e}")
        
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
        Load historical weather observations from Avro files
        
        Schema for temp: timeObserved, stationId, stationName, mean_temp, lon, lat, dkArea, municipalityCode
        Schema for sun: timeObserved, stationId, stationName, mean_radiation, lon, lat, dkArea, municipalityCode
        Schema for wind: timeObserved, stationId, stationName, mean_wind_speed, lon, lat, dkArea, municipalityCode
        
        Path: /historical/<year>/weather-<type>/<month>.avro/part-*.avro
        
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
            for month in range(1, 13):
                path = self.paths.get_weather_path(parameter, year, month)
                
                try:
                    df = self.spark.read.format("avro").load(path)
                    
                    # Parse timestamp (timeObserved)
                    df = df.withColumn("timestamp", F.to_timestamp("timeObserved"))
                    
                    # Rename value column to standard 'value' for processing
                    if value_col in df.columns:
                        df = df.withColumn("value", F.col(value_col))
                    
                    # Add parameter column if not present
                    if "parameter" not in df.columns:
                        df = df.withColumn("parameter", F.lit(parameter))
                    
                    dfs.append(df)
                    logger.debug(f"Loaded {path}")
                except Exception as e:
                    logger.debug(f"Could not load {path}: {e}")
        
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
        Load forecast weather data with fallback logic
        
        Schema same as historical weather
        
        Strategy:
        1. If no specific dates: Load from /live/forecast/weather-<type>/part-*.avro
        2. If specific dates: 
           a) Try /historical/<year>/forecast-<type>/<month>/<day-HH-MM>_batch-*_<uuid>/part-*.avro
           b) Fallback to /historical/<year>/weather-<type>/<month>.avro/part-*.avro
        
        Args:
            parameter: Weather parameter
            specific_dates: Optional list of (year, month, day) tuples
        
        Returns:
            Spark DataFrame with forecast/weather data
        """
        if specific_dates is None:
            # No specific dates - load from live forecast
            return self._load_live_forecast(parameter)
        else:
            # Specific dates - try archived forecast first, then historical weather
            return self._load_forecast_for_dates(parameter, specific_dates)
    
    def _load_live_forecast(self, parameter: str) -> DataFrame:
        """
        Load live forecast from current cycle accumulation
        
        Schema same as historical weather
        
        Path: /live/forecast/weather-<type>/part-*.avro
        """
        path = self.paths.get_live_forecast_path(parameter)
        
        logger.info(f"Loading live forecast {parameter} from {path}")
        
        # Map parameter to value column
        value_column_map = {
            'temperature-2m': 'mean_temp',
            'direct-solar-exposure': 'mean_radiation',
            'wind-speed-10m': 'mean_wind_speed'
        }
        value_col = value_column_map.get(parameter, 'value')
        
        try:
            # Read all part-*.avro files in directory
            df = self.spark.read.format("avro").load(path)
            
            # Parse timestamp (timeObserved)
            df = df.withColumn("timestamp", F.to_timestamp("timeObserved"))
            
            # Rename value column to standard 'value' for processing
            if value_col in df.columns:
                df = df.withColumn("value", F.col(value_col))
            
            # Add parameter column if not present
            if "parameter" not in df.columns:
                df = df.withColumn("parameter", F.lit(parameter))
            
            logger.info(f"Loaded {df.count():,} live forecast records for {parameter}")
            return df
        except Exception as e:
            logger.error(f"Error loading live forecast from {path}: {e}")
            raise
    
    def _load_forecast_for_dates(
        self,
        parameter: str,
        specific_dates: List[Tuple[int, int, int]]
    ) -> DataFrame:
        """
        Load forecast for specific dates with fallback to historical weather
        
        Args:
            parameter: Weather parameter
            specific_dates: List of (year, month, day) tuples
        
        Returns:
            DataFrame with forecast/weather data
        """
        logger.info(f"Loading forecast for {len(specific_dates)} specific dates")
        
        # Group dates by year and month
        dates_by_year_month = {}
        for year, month, day in specific_dates:
            key = (year, month)
            if key not in dates_by_year_month:
                dates_by_year_month[key] = []
            dates_by_year_month[key].append(day)
        
        all_dfs = []
        
        for (year, month), days in dates_by_year_month.items():
            # Try archived forecast first
            archived_df = self._try_load_archived_forecast(parameter, year, month, days)
            
            if archived_df is not None and archived_df.count() > 0:
                logger.info(f"Using archived forecast for {year}-{month:02d}")
                all_dfs.append(archived_df)
            else:
                # Fallback to historical weather
                logger.info(f"Archived forecast not found for {year}-{month:02d}, using historical weather")
                historical_df = self._load_historical_weather_for_month(parameter, year, month, days)
                if historical_df is not None:
                    all_dfs.append(historical_df)
        
        if not all_dfs:
            raise ValueError(f"No forecast or historical weather found for specified dates")
        
        # Union all dataframes
        result = all_dfs[0]
        for df in all_dfs[1:]:
            result = result.unionByName(df, allowMissingColumns=True)
        
        logger.info(f"Loaded {result.count():,} forecast/weather records for {parameter}")
        return result
    
    def _try_load_archived_forecast(
        self,
        parameter: str,
        year: int,
        month: int,
        days: List[int]
    ) -> Optional[DataFrame]:
        """
        Try to load archived forecast batches
        
        Path: /historical/<year>/forecast-<type>/<month>/<day-HH-MM>_batch-*_<uuid>/part-*.avro
        
        Args:
            parameter: Weather parameter
            year: Year
            month: Month
            days: List of days
        
        Returns:
            DataFrame if found, None otherwise
        """
        base_path = self.paths.get_archived_forecast_path(parameter, year, month)
        
        logger.debug(f"Trying archived forecast: {base_path}")
        
        # Map parameter to value column
        value_column_map = {
            'temperature-2m': 'mean_temp',
            'direct-solar-exposure': 'mean_radiation',
            'wind-speed-10m': 'mean_wind_speed'
        }
        value_col = value_column_map.get(parameter, 'value')
        
        try:
            # Try to read all subdirectories (batch folders)
            full_path = f"{base_path}/*"
            
            df = self.spark.read.format("avro").load(full_path)
            
            # Parse timestamp (timeObserved)
            df = df.withColumn("timestamp", F.to_timestamp("timeObserved"))
            
            # Rename value column to standard 'value'
            if value_col in df.columns:
                df = df.withColumn("value", F.col(value_col))
            
            # Add parameter column if not present
            if "parameter" not in df.columns:
                df = df.withColumn("parameter", F.lit(parameter))
            
            # Filter to specific days
            day_conditions = [F.dayofmonth("timestamp") == day for day in days]
            combined_condition = day_conditions[0]
            for condition in day_conditions[1:]:
                combined_condition = combined_condition | condition
            
            df = df.filter(combined_condition)
            
            if df.count() > 0:
                logger.info(f"Found {df.count()} records in archived forecast batches")
                return df
            else:
                logger.debug(f"No data found in archived forecast for specified days")
                return None
        except Exception as e:
            logger.debug(f"Archived forecast not found: {e}")
            return None
    
    def _load_historical_weather_for_month(
        self,
        parameter: str,
        year: int,
        month: int,
        days: List[int]
    ) -> Optional[DataFrame]:
        """
        Load historical weather observations for specific days in a month
        
        Path: /historical/<year>/weather-<type>/<month>.avro/part-*.avro
        
        Args:
            parameter: Weather parameter
            year: Year
            month: Month
            days: List of days
        
        Returns:
            DataFrame if found, None otherwise
        """
        path = self.paths.get_weather_path(parameter, year, month)
        
        logger.debug(f"Loading historical weather from {path}")
        
        # Map parameter to value column
        value_column_map = {
            'temperature-2m': 'mean_temp',
            'direct-solar-exposure': 'mean_radiation',
            'wind-speed-10m': 'mean_wind_speed'
        }
        value_col = value_column_map.get(parameter, 'value')
        
        try:
            df = self.spark.read.format("avro").load(path)
            
            # Parse timestamp (timeObserved)
            df = df.withColumn("timestamp", F.to_timestamp("timeObserved"))
            
            # Rename value column to standard 'value'
            if value_col in df.columns:
                df = df.withColumn("value", F.col(value_col))
            
            # Add parameter column if not present
            if "parameter" not in df.columns:
                df = df.withColumn("parameter", F.lit(parameter))
            
            # Filter to specific days
            day_conditions = [F.dayofmonth("timestamp") == day for day in days]
            combined_condition = day_conditions[0]
            for condition in day_conditions[1:]:
                combined_condition = combined_condition | condition
            
            df = df.filter(combined_condition)
            
            if df.count() > 0:
                logger.info(f"Loaded {df.count()} records from historical weather")
                return df
            else:
                logger.warning(f"No data found in historical weather for specified days")
                return None
        except Exception as e:
            logger.warning(f"Could not load historical weather: {e}")
            return None
    
    def get_forecast_dates(self) -> List[Tuple[int, int, int]]:
        """
        Get list of unique dates from live forecast data
        
        Returns:
            List of (year, month, day) tuples
        """
        logger.info("Extracting forecast dates from live data...")
        
        # Load one forecast to get dates
        temp_df = self._load_live_forecast('temperature-2m')
        
        # Extract unique dates
        dates = temp_df.select(
            F.year("timestamp").alias("year"),
            F.month("timestamp").alias("month"),
            F.dayofmonth("timestamp").alias("day")
        ).distinct().collect()
        
        date_list = [(row.year, row.month, row.day) for row in dates]
        date_list.sort()
        
        logger.info(f"Found {len(date_list)} unique forecast dates")
        return date_list
    
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