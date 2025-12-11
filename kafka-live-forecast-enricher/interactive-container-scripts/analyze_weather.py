from pyspark.sql import SparkSession

# Add Avro package configuration
spark = SparkSession.builder \
    .appName("WeatherAnalysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-g5:9000") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.1") \
    .getOrCreate()

print("=== Reading wind data ===")
wind = spark.read.format("avro").load("hdfs://namenode-g5:9000/raw/forecast/weather-wind")

print("\n=== Reading temp data ===")
temp = spark.read.format("avro").load("hdfs://namenode-g5:9000/raw/forecast/weather-temp")

print("\n=== Reading sun data ===")
sun = spark.read.format("avro").load("hdfs://namenode-g5:9000/raw/forecast/weather-sun")

print(f"\nWind records: {wind}")
print(f"Temp records: {temp}")
print(f"Sun records: {sun}")

print("\n=== Wind DK Area Distribution ===")
wind.groupBy("dkArea").count().show()

print("\n=== Temp Municipality Distribution (Top 10) ===")
temp.groupBy("municipalityCode").count().orderBy("count", ascending=False).show(10)

spark.stop()
