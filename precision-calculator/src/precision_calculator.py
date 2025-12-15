from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, abs as spark_abs, lit


class PrecisionCalculator:
    """Handles calculation of prediction precision."""
    
    def __init__(self, spark: SparkSession, hdfs_namenode: str):
        self.spark = spark
        self.hdfs_namenode = hdfs_namenode
    
    def calculate_precision(self, predictions_df, real_consumption_df, real_price_df):
        """
        Calculate precision for predictions by joining with real data.
        
        Args:
            predictions_df: DataFrame with prediction data
            real_consumption_df: DataFrame with real consumption data (or None)
            real_price_df: DataFrame with real price data (or None)
        
        Returns:
            DataFrame with precision columns added
        """
        result_df = predictions_df
        
        # Add dkArea if it doesn't exist (based on municipalityCode)
        # municipalityCode > 400 -> dkArea = 1, else dkArea = 2
        if "dkArea" not in result_df.columns:
            print("  Adding dkArea column based on municipalityCode...")
            result_df = result_df.withColumn(
                "dkArea",
                when(col("municipalityCode") > 400, 1).otherwise(2)
            )
        
        # Join with real consumption data if available
        if real_consumption_df is not None:
            print("  Joining with real consumption data...")
            # Rename the real consumption column to avoid conflicts
            real_consumption_df = real_consumption_df.withColumnRenamed("realConsumptionKwh", "realConsumptionKwh_new")
            
            # Use explicit join condition instead of 'on' parameter
            result_df = result_df.join(
                real_consumption_df,
                (result_df["timestamp"] == real_consumption_df["timestamp"]) & 
                (result_df["municipalityCode"] == real_consumption_df["municipalityCode"]),
                how="left"
            ).drop(real_consumption_df["timestamp"]).drop(real_consumption_df["municipalityCode"])
            
            # Use the new column if it exists, otherwise keep the old one (or null)
            result_df = result_df.withColumn(
                "realConsumptionKwh",
                when(col("realConsumptionKwh_new").isNotNull(), col("realConsumptionKwh_new"))
                .otherwise(col("realConsumptionKwh") if "realConsumptionKwh" in predictions_df.columns else lit(None))
            ).drop("realConsumptionKwh_new")
        elif "realConsumptionKwh" not in result_df.columns:
            # Add placeholder column if it doesn't exist
            result_df = result_df.withColumn("realConsumptionKwh", lit(None).cast("double"))
        
        # Join with real price data if available
        if real_price_df is not None:
            print("  Joining with real price data...")
            # Rename the real price column to avoid conflicts
            real_price_df = real_price_df.withColumnRenamed("realPrice_EUR_MWh", "realPrice_EUR_MWh_new")
            
            # Use explicit join condition instead of 'on' parameter
            result_df = result_df.join(
                real_price_df,
                (result_df["timestamp"] == real_price_df["timestamp"]) & 
                (result_df["dkArea"] == real_price_df["dkArea"]),
                how="left"
            ).drop(real_price_df["timestamp"]).drop(real_price_df["dkArea"])
            
            # Use the new column if it exists, otherwise keep the old one (or null)
            result_df = result_df.withColumn(
                "realPrice_EUR_MWh",
                when(col("realPrice_EUR_MWh_new").isNotNull(), col("realPrice_EUR_MWh_new"))
                .otherwise(col("realPrice_EUR_MWh") if "realPrice_EUR_MWh" in predictions_df.columns else lit(None))
            ).drop("realPrice_EUR_MWh_new")
        elif "realPrice_EUR_MWh" not in result_df.columns:
            # Add placeholder column if it doesn't exist
            result_df = result_df.withColumn("realPrice_EUR_MWh", lit(None).cast("double"))
        
        # Calculate consumption precision
        print("  Calculating consumption precision...")
        result_df = result_df.withColumn(
            "consumptionPrecision",
            when(
                (col("realConsumptionKwh").isNull()) | (col("realConsumptionKwh") == 0.0),
                0.0
            ).otherwise(
                when(
                    100.0 * (1.0 - spark_abs(col("consumptionkWh") - col("realConsumptionKwh")) / col("realConsumptionKwh")) < 0.0,
                    0.0
                ).otherwise(
                    100.0 * (1.0 - spark_abs(col("consumptionkWh") - col("realConsumptionKwh")) / col("realConsumptionKwh"))
                )
            )
        )
        
        # Calculate price precision
        print("  Calculating price precision...")
        result_df = result_df.withColumn(
            "pricePrecision",
            when(
                (col("realPrice_EUR_MWh").isNull()) | (col("realPrice_EUR_MWh") == 0.0),
                0.0
            ).otherwise(
                when(
                    100.0 * (1.0 - spark_abs(col("price") - col("realPrice_EUR_MWh")) / col("realPrice_EUR_MWh")) < 0.0,
                    0.0
                ).otherwise(
                    100.0 * (1.0 - spark_abs(col("price") - col("realPrice_EUR_MWh")) / col("realPrice_EUR_MWh"))
                )
            )
        )
        
        print("  ✓ Precision calculation complete")
        
        return result_df