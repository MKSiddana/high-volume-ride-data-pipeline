from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)

spark = (
    SparkSession.builder
    .appName("RideDataIngestion")
    .getOrCreate()
)

# Explicit schema for ride data
ride_schema = StructType([
    StructField("ride_id", StringType(), False),
    StructField("driver_id", StringType(), False),
    StructField("rider_id", StringType(), False),
    StructField("city", StringType(), True),
    StructField("pickup_timestamp", TimestampType(), True),
    StructField("dropoff_timestamp", TimestampType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("ride_status", StringType(), True),
    StructField("last_updated", TimestampType(), True)
])

# Source location
input_path = "data/sample_rides.csv"

# Read source data
rides_df = (
    spark.read
    .option("header", "true")
    .schema(ride_schema)
    .csv(input_path)
)

print("Source record count:", rides_df.count())

rides_df.printSchema()
rides_df.show(truncate=False)

# Add ingestion date for partitioning
from pyspark.sql.functions import to_date, col

rides_df = rides_df.withColumn(
    "ingestion_date",
    to_date(col("pickup_timestamp"))
)

# Write raw data as partitioned Parquet
raw_path = "output/raw/rides"

(
    rides_df.write
    .mode("overwrite")
    .partitionBy("ingestion_date")
    .parquet(raw_path)
)

print("Ride data ingestion completed successfully.")
