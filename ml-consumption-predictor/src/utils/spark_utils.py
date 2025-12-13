"""
Spark utilities and session management
"""

from pyspark.sql import SparkSession
from pyspark import SparkConf
import logging
import os

logger = logging.getLogger(__name__)


class SparkSessionManager:
    """Manages Spark session lifecycle"""
    
    _instance = None
    _spark = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_spark_session(self) -> SparkSession:
        """Get or create Spark session"""
        if self._spark is None:
            self._spark = self._create_spark_session()
        return self._spark
    
    def _create_spark_session(self) -> SparkSession:
        """Create new Spark session with optimized settings"""
        logger.info("Creating Spark session...")
        
        # Import settings here to avoid circular dependency
        from src.config.settings import settings
        
        conf = SparkConf()
        conf.set("spark.app.name", settings.spark.app_name)
        
        # Set master only if not already set by spark-submit
        if not conf.get("spark.master", None):
            conf.set("spark.master", settings.spark.master)
        
        conf.set("spark.executor.memory", settings.spark.executor_memory)
        conf.set("spark.driver.memory", settings.spark.driver_memory)
        conf.set("spark.executor.cores", str(settings.spark.executor_cores))
        
        # Parquet optimizations
        conf.set("spark.sql.parquet.compression.codec", settings.spark.parquet_compression)
        conf.set("spark.sql.parquet.enableVectorizedReader", "true")
        
        # Memory and performance optimizations
        conf.set("spark.sql.shuffle.partitions", "200")
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Disable Arrow to avoid compatibility issues
        # Arrow can cause "sun.misc.Unsafe" errors on some systems
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        
        # Avro support
        conf.set("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0")
        
        # Kubernetes specific settings
        if settings.is_kubernetes:
            logger.info("Configuring for Kubernetes deployment...")
            conf.set("spark.kubernetes.allocation.batch.size", "10")
            conf.set("spark.kubernetes.container.image.pullPolicy", "IfNotPresent")
            conf.set("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")
        
        spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Spark session created: {spark.version}")
        logger.info(f"Spark master: {spark.conf.get('spark.master')}")
        logger.info(f"Arrow enabled: {spark.conf.get('spark.sql.execution.arrow.pyspark.enabled')}")
        
        return spark
    
    def stop(self):
        """Stop Spark session"""
        if self._spark is not None:
            logger.info("Stopping Spark session...")
            self._spark.stop()
            self._spark = None


# Singleton instance
spark_manager = SparkSessionManager()


def get_spark() -> SparkSession:
    """Convenience function to get Spark session"""
    return spark_manager.get_spark_session()