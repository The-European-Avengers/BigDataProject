from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExportJSON") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-g5:9000") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.1") \
    .getOrCreate()

# Read data
wind = spark.read.format("avro").load("hdfs://namenode-g5:9000/raw/forecast/weather-wind")

# Save 1000 records as JSON
print("Saving 1000 records as JSON...")
wind.limit(1000).coalesce(1).write.json(
    "hdfs://namenode-g5:9000/tmp/wind_data.json",
    mode="overwrite"
)

print("Done! JSON saved to hdfs://namenode-g5:9000/tmp/wind_data.json")

spark.stop()