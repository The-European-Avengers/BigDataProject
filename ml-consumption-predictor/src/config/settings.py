"""
Configuration settings for Energy Consumption ML Predictor
Supports both local and Kubernetes deployment modes
"""

import os
from dataclasses import dataclass
from typing import List, Optional
from enum import Enum
from pathlib import Path


class DeploymentMode(Enum):
    """Deployment mode"""
    LOCAL = "local"
    KUBERNETES = "kubernetes"


@dataclass
class LocalPathConfig:
    """Local filesystem path configuration"""
    
    # Get project root (parent of src/)
    project_root: Path = None
    
    def __post_init__(self):
        """Initialize paths relative to project root"""
        if self.project_root is None:
            # Get project root: go up from src/config/settings.py
            current_file = Path(__file__).resolve()  # .../src/config/settings.py
            self.project_root = current_file.parent.parent.parent  # .../ml-consumption-predictor
        
        # Define data paths
        self.data_root = self.project_root / "data"
        self.csvs_root = self.data_root / "csvs"
        self.consumption_path = self.csvs_root / "consumption"
        self.weather_path = self.csvs_root / "weather"
        self.forecast_path = self.csvs_root / "forecast"
        self.production_path = self.csvs_root / "production"
        self.price_path = self.csvs_root / "price"
        self.analytics_path = self.data_root / "analytics"
        
        # Create directories if they don't exist
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create necessary directories"""
        for path in [
            self.consumption_path,
            self.weather_path,
            self.forecast_path,
            self.production_path,
            self.price_path,
            self.analytics_path
        ]:
            path.mkdir(parents=True, exist_ok=True)
    
    def get_consumption_path(self, year: int) -> str:
        """Get consumption file path for a year"""
        return str(self.consumption_path / f"{year}.csv")
    
    def get_weather_path(self, parameter: str, year: int) -> str:
        """Get weather file path for a parameter and year"""
        param_map = {
            'temperature-2m': 'temp',
            'direct-solar-exposure': 'sun',
            'wind-speed-10m': 'wind'
        }
        param_short = param_map.get(parameter, parameter)
        return str(self.weather_path / f"{param_short}_{year}.csv")
    
    def get_forecast_path(self, parameter: str) -> str:
        """Get forecast file path for a parameter"""
        param_map = {
            'temperature-2m': 'temp',
            'direct-solar-exposure': 'sun',
            'wind-speed-10m': 'wind'
        }
        param_short = param_map.get(parameter, parameter)
        return str(self.forecast_path / f"{param_short}.csv")
    
    def get_production_path(self, year: int) -> str:
        """Get production file path for a year"""
        return str(self.production_path / f"{year}.csv")
    
    def get_price_path(self, year: int) -> str:
        """Get price file path for a year"""
        return str(self.price_path / f"{year}.csv")
    
    def get_analytics_path(self, year: int, month: int, day: int) -> str:
        """Get analytics output path"""
        return str(self.analytics_path / f"{year}-{month:02d}-{day:02d}.csv")


@dataclass
class K8sPathConfig:
    """Kubernetes HDFS path configuration"""
    # Base path
    base_path: str = os.getenv('DATA_BASE_PATH', 'hdfs://namenode-g5:9000')
    
    def get_consumption_path(self, year: int, month: int) -> str:
        """
        Historical consumption path
        Format: /historical/<year>/consumption/<month>.avro/part-*.avro
        """
        return f"{self.base_path}/historical/{year}/consumption/{month:02d}.avro"
    
    def get_weather_path(self, parameter: str, year: int, month: int) -> str:
        """
        Historical weather observations path
        Format: /historical/<year>/weather-<type>/<month>.avro/part-*.avro
        """
        param_map = {
            'temperature-2m': 'temp',
            'direct-solar-exposure': 'sun',
            'wind-speed-10m': 'wind'
        }
        param_short = param_map.get(parameter, parameter)
        return f"{self.base_path}/historical/{year}/weather-{param_short}/{month:02d}.avro"
    
    def get_production_path(self, year: int, month: int) -> str:
        """
        Historical production path
        Format: /historical/<year>/production/<month>.avro/part-*.avro
        """
        return f"{self.base_path}/historical/{year}/production/{month:02d}.avro"
    
    def get_price_path(self, year: int, month: int) -> str:
        """
        Historical price path
        Format: /historical/<year>/price/<month>.avro/part-*.avro
        """
        return f"{self.base_path}/historical/{year}/price/{month:02d}.avro"
    
    def get_live_forecast_path(self, parameter: str) -> str:
        """
        Live forecast path (current cycle accumulation)
        Format: /live/forecast/weather-<type>/part-*.avro
        """
        param_map = {
            'temperature-2m': 'temp',
            'direct-solar-exposure': 'sun',
            'wind-speed-10m': 'wind'
        }
        param_short = param_map.get(parameter, parameter)
        return f"{self.base_path}/live/forecast/weather-{param_short}"
    
    def get_archived_forecast_path(self, parameter: str, year: int, month: int) -> str:
        """
        Archived forecast path (specific forecast cycles)
        Format: /historical/<year>/forecast-<type>/<month>/<day-HH-MM>_batch-*_<uuid>/part-*.avro
        
        Note: This returns the base path. Individual cycles are in subdirectories.
        """
        param_map = {
            'temperature-2m': 'temp',
            'direct-solar-exposure': 'sun',
            'wind-speed-10m': 'wind'
        }
        param_short = param_map.get(parameter, parameter)
        return f"{self.base_path}/historical/{year}/forecast-{param_short}/{month:02d}"
    
    def get_analytics_path(self, year: int, month: int, day: int) -> str:
        """
        Current analytics output path
        Format: /analytics/<year>-<month>-<day>.parquet
        """
        return f"{self.base_path}/analytics/{year}-{month:02d}-{day:02d}.parquet"
    
    def get_archive_analytics_path(self, year: int, month: int, uuid: str) -> str:
        """
        Archived analytics path
        Format: /historical/archives/<year>/<month>/analytics/<uuid>.parquet
        """
        return f"{self.base_path}/historical/archives/{year}/{month:02d}/analytics/{uuid}.parquet"


@dataclass
class ModelConfig:
    """ML model configuration"""
    # Training parameters
    default_training_years: int = 4
    
    # XGBoost hyperparameters
    n_estimators: int = 200
    learning_rate: float = 0.05
    max_depth: int = 6
    min_child_weight: int = 3
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    random_state: int = 42
    
    # Feature configuration
    feature_columns: List[str] = None
    price_feature_columns: List[str] = None
    
    def __post_init__(self):
        """Initialize feature columns"""
        if self.feature_columns is None:
            self.feature_columns = [
                'municipalityCode', 'hour', 'day_of_week', 'month', 'day_of_year',
                'is_weekend', 'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
                'temperature', 'sunlight', 'temp_x_sunlight', 'temp_squared',
                'is_cold', 'is_dark', 'cold_and_dark',
                'consumption_same_hour_last_year', 'consumption_same_day_last_year'
            ]
        
        if self.price_feature_columns is None:
            self.price_feature_columns = [
                'hour', 'day_of_week', 'month', 'day_of_year',
                'is_weekend', 'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
                'total_consumption', 'total_production',
                'wind_production', 'solar_production',
                'production_ratio', 'net_demand'
            ]


@dataclass
class SparkConfig:
    """Spark configuration"""
    app_name: str = "EnergyConsumptionMLPredictor"
    
    # Spark master (auto-detect or from env)
    master: str = None
    
    # Spark configurations
    executor_memory: str = os.getenv('SPARK_EXECUTOR_MEMORY', '4g')
    driver_memory: str = os.getenv('SPARK_DRIVER_MEMORY', '4g')
    executor_cores: int = int(os.getenv('SPARK_EXECUTOR_CORES', '2'))
    
    # Parquet settings
    parquet_compression: str = 'snappy'
    
    def __post_init__(self):
        """Set master based on mode if not specified"""
        if self.master is None:
            # Will be set dynamically based on deployment mode
            self.master = 'local[*]'


class Settings:
    """Main settings container"""
    
    def __init__(self, mode: Optional[DeploymentMode] = None):
        """
        Initialize settings
        
        Args:
            mode: Deployment mode (local or kubernetes). 
                  If None, auto-detect from environment.
        """
        # Auto-detect mode if not specified
        if mode is None:
            mode = self._detect_mode()
        
        self.mode = mode
        self.model = ModelConfig()
        self.spark = SparkConfig()
        
        # Set paths based on mode
        if self.mode == DeploymentMode.LOCAL:
            self.paths = LocalPathConfig()
            self.spark.master = 'local[*]'
        else:
            self.paths = K8sPathConfig()
            # K8s master will be set by spark-submit
            self.spark.master = os.getenv('SPARK_MASTER', 'local[*]')
    
    def _detect_mode(self) -> DeploymentMode:
        """Auto-detect deployment mode from environment"""
        # Check for Kubernetes service account
        if os.path.exists('/var/run/secrets/kubernetes.io'):
            return DeploymentMode.KUBERNETES
        
        # Check for explicit environment variable
        mode_env = os.getenv('DEPLOYMENT_MODE', 'kubernetes').lower()
        if mode_env == 'local':
            return DeploymentMode.LOCAL
        
        return DeploymentMode.KUBERNETES
    
    @property
    def is_local(self) -> bool:
        """Check if running in local mode"""
        return self.mode == DeploymentMode.LOCAL
    
    @property
    def is_kubernetes(self) -> bool:
        """Check if running in Kubernetes"""
        return self.mode == DeploymentMode.KUBERNETES


# Global settings instance (will be initialized with proper mode in spark_job.py)
settings: Optional[Settings] = None


def initialize_settings(mode: Optional[DeploymentMode] = None) -> Settings:
    """
    Initialize global settings
    
    Args:
        mode: Deployment mode
    
    Returns:
        Settings instance
    """
    global settings
    settings = Settings(mode)
    return settings