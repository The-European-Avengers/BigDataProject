"""
Data validation utilities to handle missing data
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from datetime import datetime
from typing import List, Tuple, Dict
import logging

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates data completeness and adjusts date ranges"""
    
    @staticmethod
    def find_valid_date_range(
        consumption_df: DataFrame,
        weather_dfs: Dict[str, DataFrame],
        requested_years: int
    ) -> Tuple[datetime, datetime, int]:
        """
        Find the valid date range where we have complete data
        
        Args:
            consumption_df: Consumption DataFrame
            weather_dfs: Dictionary of weather DataFrames (temp, sun, wind)
            requested_years: Number of years requested
        
        Returns:
            Tuple of (start_date, end_date, actual_years_loaded)
        """
        logger.info("Validating data completeness...")
        
        # Get date ranges for each dataset
        consumption_range = DataValidator._get_date_range(consumption_df, 'TimeDK')
        
        weather_ranges = {}
        for param, df in weather_dfs.items():
            weather_ranges[param] = DataValidator._get_date_range(df, 'timestamp')
        
        # Find the latest start date (most restrictive)
        all_starts = [consumption_range[0]] + [r[0] for r in weather_ranges.values()]
        valid_start = max(all_starts)
        
        # Find the earliest end date (most restrictive)
        all_ends = [consumption_range[1]] + [r[1] for r in weather_ranges.values()]
        valid_end = min(all_ends)
        
        logger.info(f"Consumption range: {consumption_range[0]} to {consumption_range[1]}")
        for param, range_dates in weather_ranges.items():
            logger.info(f"{param} range: {range_dates[0]} to {range_dates[1]}")
        
        logger.info(f"Valid overlapping range: {valid_start} to {valid_end}")
        
        # Calculate actual years of data
        years_diff = (valid_end - valid_start).days / 365.25
        actual_years = int(years_diff)
        
        if actual_years < requested_years:
            logger.warning(
                f"Only {actual_years} years of complete data available "
                f"(requested {requested_years}). Proceeding with available data."
            )
        
        return valid_start, valid_end, actual_years
    
    @staticmethod
    def _get_date_range(df: DataFrame, time_col: str) -> Tuple[datetime, datetime]:
        """Get min and max dates from DataFrame"""
        result = df.select(
            F.min(time_col).alias('min_date'),
            F.max(time_col).alias('max_date')
        ).collect()[0]
        
        return (result.min_date, result.max_date)
    
    @staticmethod
    def filter_by_date_range(
        df: DataFrame,
        time_col: str,
        start_date: datetime,
        end_date: datetime
    ) -> DataFrame:
        """Filter DataFrame to date range"""
        return df.filter(
            (F.col(time_col) >= F.lit(start_date)) &
            (F.col(time_col) <= F.lit(end_date))
        )
    
    @staticmethod
    def check_hourly_completeness(
        df: DataFrame,
        time_col: str,
        tolerance_hours: int = 2
    ) -> bool:
        """
        Check if data is reasonably complete (allows some missing hours)
        
        Args:
            df: DataFrame with time column
            time_col: Name of time column
            tolerance_hours: Maximum allowed missing hours
        
        Returns:
            True if data is complete enough
        """
        date_range = DataValidator._get_date_range(df, time_col)
        start, end = date_range
        
        # Expected hours
        expected_hours = int((end - start).total_seconds() / 3600) + 1
        
        # Actual count
        actual_count = df.count()
        
        missing_hours = expected_hours - actual_count
        
        logger.info(
            f"Expected {expected_hours} hours, found {actual_count} "
            f"({missing_hours} missing)"
        )
        
        if missing_hours > tolerance_hours:
            logger.warning(
                f"Missing {missing_hours} hours of data (tolerance: {tolerance_hours})"
            )
            return False
        
        return True