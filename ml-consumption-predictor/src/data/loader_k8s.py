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
                    if "timeUTC" in df.columns:
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
                    
                    # FIX: Check which timestamp column exists and use it
                    if "timeObserved" in df.columns:
                        df = df.withColumn("timestamp", F.to_timestamp("timeObserved"))
                    elif "timestamp" not in df.columns:
                        logger.warning(f"No timestamp column found in {path}")
                        continue
                    else:
                        # timestamp already exists, ensure it's the right type
                        df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
                    
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
    
    def load_historical_production(self, years: List[int]) -> DataFrame:
        """
        Load historical production data from Avro files
        
        Path: /historical/<year>/production/<month>.avro/part-*.avro
        
        Args:
            years: List of years to load
        
        Returns:
            Spark DataFrame with production data
        """
        logger.info(f"Loading production data for years: {years}")
        
        dfs = []
        for year in years:
            for month in range(1, 13):
                path = self.paths.get_production_path(year, month)
                
                try:
                    df = self.spark.read.format("avro").load(path)
                    
                    # Parse timestamp
                    df = df.withColumn("timeObserved", F.to_timestamp("timeObserved"))
                    
                    dfs.append(df)
                    logger.debug(f"Loaded {path}")
                except Exception as e:
                    logger.debug(f"Could not load {path}: {e}")
        
        if not dfs:
            raise ValueError(f"No production data found for years {years}")
        
        # Union all dataframes
        result = dfs[0]
        for df in dfs[1:]:
            result = result.unionByName(df, allowMissingColumns=True)
        
        logger.info(f"Loaded {result.count():,} production records")
        return result
    
    def load_historical_price(self, years: List[int]) -> DataFrame:
        """
        Load historical price data from Avro files
        
        Path: /historical/<year>/price.avro (one file per year)
        
        Args:
            years: List of years to load
        
        Returns:
            Spark DataFrame with price data
        """
        logger.info(f"Loading price data for years: {years}")
        
        dfs = []
        for year in years:
            path = self.paths.get_price_path(year)
            
            try:
                df = self.spark.read.format("avro").load(path)
                
                # Parse timestamp
                df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
                
                dfs.append(df)
                logger.debug(f"Loaded {path}")
            except Exception as e:
                logger.debug(f"Could not load {path}: {e}")
        
        if not dfs:
            raise ValueError(f"No price data found for years {years}")
        
        # Union all dataframes
        result = dfs[0]
        for df in dfs[1:]:
            result = result.unionByName(df, allowMissingColumns=True)
        
        logger.info(f"Loaded {result.count():,} price records")
        return result
    
    def load_forecast_weather(
        self,
        parameter: str,
        specific_dates: Optional[List[Tuple[int, int, int]]] = None
    ) -> DataFrame:
        """
        Load forecast weather data with fallback logic
        
        Strategy:
        1. If no specific dates: Load ALL from /live/forecast/weather-<type>/part-*.avro
        2. If specific dates: 
           a) Try loading from /live/forecast/ and filter to dates
           b) If dates not found, fallback to /historical/<year>/weather-<type>/<month>.avro
        
        Args:
            parameter: Weather parameter
            specific_dates: Optional list of (year, month, day) tuples
        
        Returns:
            Spark DataFrame with forecast/weather data
        """
        if specific_dates is None:
            # No specific dates - load ALL from live forecast
            return self._load_live_forecast(parameter)
        else:
            # Specific dates - try live forecast first, then historical weather
            return self._load_forecast_for_dates(parameter, specific_dates)
    
    def _load_live_forecast(self, parameter: str) -> DataFrame:
        """
        Load ALL timestamps from live forecast
        
        Path: /live/forecast/weather-<type>/part-*.avro
        
        FIX: Forecast data has 'timestamp' column, not 'timeObserved'
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
            
            # FIX: Forecast data already has 'timestamp' column, no need to rename
            # Just ensure it's the right type
            if "timestamp" in df.columns:
                df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
            else:
                logger.error(f"No timestamp column in forecast data. Available columns: {df.columns}")
                raise ValueError(f"Forecast data missing timestamp column")
            
            # Rename value column to standard 'value' for processing
            if value_col in df.columns:
                df = df.withColumn("value", F.col(value_col))
            elif "value" not in df.columns:
                logger.error(f"No value column found. Available columns: {df.columns}")
                raise ValueError(f"Forecast data missing value column")
            
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
        
        # First, try to load from live forecast and filter
        try:
            live_df = self._load_live_forecast(parameter)
            
            # Check which dates are available in live forecast
            available_dates = live_df.select(
                F.year("timestamp").alias("year"),
                F.month("timestamp").alias("month"),
                F.dayofmonth("timestamp").alias("day")
            ).distinct().collect()
            
            available_set = {(row.year, row.month, row.day) for row in available_dates}
            requested_set = set(specific_dates)
            
            found_in_live = requested_set.intersection(available_set)
            missing_dates = requested_set - available_set
            
            logger.info(f"Found {len(found_in_live)} dates in live forecast")
            logger.info(f"Missing {len(missing_dates)} dates, will load from historical")
            
            # Filter live forecast to requested dates that exist
            if found_in_live:
                date_conditions = []
                for year, month, day in found_in_live:
                    condition = (
                        (F.year("timestamp") == year) &
                        (F.month("timestamp") == month) &
                        (F.dayofmonth("timestamp") == day)
                    )
                    date_conditions.append(condition)
                
                combined_condition = date_conditions[0]
                for condition in date_conditions[1:]:
                    combined_condition = combined_condition | condition
                
                live_filtered = live_df.filter(combined_condition)
            else:
                live_filtered = None
            
            # Load missing dates from historical weather
            historical_dfs = []
            if missing_dates:
                historical_dfs = self._load_historical_for_dates(parameter, list(missing_dates))
            
            # Combine results
            all_dfs = []
            if live_filtered is not None and live_filtered.count() > 0:
                all_dfs.append(live_filtered)
            all_dfs.extend(historical_dfs)
            
            if not all_dfs:
                raise ValueError(f"No data found for specified dates")
            
            result = all_dfs[0]
            for df in all_dfs[1:]:
                result = result.unionByName(df, allowMissingColumns=True)
            
            logger.info(f"Loaded {result.count():,} total records for {parameter}")
            return result
            
        except Exception as e:
            logger.warning(f"Could not load from live forecast: {e}")
            logger.info("Falling back to historical weather entirely")
            return self._load_historical_for_dates_only(parameter, specific_dates)
    
    def _load_historical_for_dates(
        self,
        parameter: str,
        dates: List[Tuple[int, int, int]]
    ) -> List[DataFrame]:
        """Load historical weather for specific dates"""
        
        # Group dates by year and month
        dates_by_year_month = {}
        for year, month, day in dates:
            key = (year, month)
            if key not in dates_by_year_month:
                dates_by_year_month[key] = []
            dates_by_year_month[key].append(day)
        
        dfs = []
        value_column_map = {
            'temperature-2m': 'mean_temp',
            'direct-solar-exposure': 'mean_radiation',
            'wind-speed-10m': 'mean_wind_speed'
        }
        value_col = value_column_map.get(parameter, 'value')
        
        for (year, month), days in dates_by_year_month.items():
            path = self.paths.get_weather_path(parameter, year, month)
            
            try:
                df = self.spark.read.format("avro").load(path)
                
                # FIX: Check which timestamp column exists
                if "timeObserved" in df.columns:
                    df = df.withColumn("timestamp", F.to_timestamp("timeObserved"))
                elif "timestamp" in df.columns:
                    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
                else:
                    logger.warning(f"No timestamp column in {path}")
                    continue
                
                # Rename value column
                if value_col in df.columns:
                    df = df.withColumn("value", F.col(value_col))
                
                # Add parameter column
                if "parameter" not in df.columns:
                    df = df.withColumn("parameter", F.lit(parameter))
                
                # Filter to specific days
                day_conditions = [F.dayofmonth("timestamp") == day for day in days]
                combined_condition = day_conditions[0]
                for condition in day_conditions[1:]:
                    combined_condition = combined_condition | condition
                
                df = df.filter(combined_condition)
                
                if df.count() > 0:
                    dfs.append(df)
                    logger.info(f"Loaded {df.count()} records from historical {year}-{month:02d}")
            except Exception as e:
                logger.warning(f"Could not load historical {path}: {e}")
        
        return dfs
    
    def _load_historical_for_dates_only(
        self,
        parameter: str,
        dates: List[Tuple[int, int, int]]
    ) -> DataFrame:
        """Load only from historical weather (no live forecast)"""
        
        dfs = self._load_historical_for_dates(parameter, dates)
        
        if not dfs:
            raise ValueError(f"No historical data found for specified dates")
        
        result = dfs[0]
        for df in dfs[1:]:
            result = result.unionByName(df, allowMissingColumns=True)
        
        return result
    
    def get_forecast_dates(self) -> List[Tuple[int, int, int]]:
        """
        Get list of ALL unique dates from live forecast data
        
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
        
        IMPORTANT: Loads historical years EXCLUDING prediction year for training,
        then adds prediction year for trend calculation.
        
        For example, if prediction year is 2025 and requested_years=3:
        - Loads 2022, 2023, 2024 for model training
        - Adds 2025 for year-over-year trend calculation
        
        Args:
            requested_years: Number of historical years to load (excluding prediction year)
            prediction_year: The year we're making predictions for
        
        Returns:
            Dictionary with 'consumption', 'temp', 'sun', 'wind', 'production', 'price' DataFrames
        """
        
        # Training years: exclude prediction year
        training_years = list(range(prediction_year - requested_years, prediction_year))
        
        # All years including prediction year for trend calculation
        all_years = list(range(prediction_year - requested_years, prediction_year + 1))
        
        logger.info(f"Attempting to load {requested_years} training years: {training_years}")
        logger.info(f"Plus prediction year {prediction_year} for trend calculation")
        logger.info(f"Total years to load: {all_years}")
        
        # Load all data (including prediction year for trends)
        consumption_df = self.load_historical_consumption(all_years)
        temp_df = self.load_historical_weather(all_years, 'temperature-2m')
        sun_df = self.load_historical_weather(all_years, 'direct-solar-exposure')
        production_df = self.load_historical_production(all_years)
        price_df = self.load_historical_price(all_years)
        
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
            requested_years + 1  # +1 because we loaded prediction year too
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
        production_df = DataValidator.filter_by_date_range(
            production_df, 'timeObserved', valid_start, valid_end
        )
        price_df = DataValidator.filter_by_date_range(
            price_df, 'timestamp', valid_start, valid_end
        )
        
        if wind_df is not None:
            wind_df = DataValidator.filter_by_date_range(
                wind_df, 'timestamp', valid_start, valid_end
            )
        
        return {
            'consumption': consumption_df,
            'temp': temp_df,
            'sun': sun_df,
            'wind': wind_df,
            'production': production_df,
            'price': price_df
        }