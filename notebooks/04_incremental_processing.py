from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)
from pyspark.sql.functions import col, to_date, upper, trim, round

spark = (
    SparkSession.builder
    .appName("RideIncrementalProcessing")
    .getOrCreate()
)

# ---------------------------------------------------------
# Define source schema
# ---------------------------------------------------------

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

# ---------------------------------------------------------
# Watermark from previous successful load
# ---------------------------------------------------------

last_watermark = "2026-01-05 23:59:59"

# ---------------------------------------------------------
# Read incremental source
# ---------------------------------------------------------

incremental_path = "data/sample_rides_incremental.csv"

incremental_df = (
    spark.read
    .option("header", "true")
    .schema(ride_schema)
    .csv(incremental_path)
)

# ---------------------------------------------------------
# Process only records newer than watermark
# ---------------------------------------------------------

new_records_df = incremental_df.filter(
    col("last_updated") > last_watermark
)

print(
    "Incremental records to process:",
    new_records_df.count()
)

# ---------------------------------------------------------
# Clean incremental records
# ---------------------------------------------------------

processed_df = (
    new_records_df
    .dropDuplicates(["ride_id"])
    .withColumn(
        "city",
        trim(col("city"))
    )
    .withColumn(
        "ride_status",
        upper(trim(col("ride_status")))
    )
    .withColumn(
        "payment_method",
        upper(trim(col("payment_method")))
    )
    .withColumn(
        "fare_amount",
        round(col("fare_amount"), 2)
    )
    .withColumn(
        "ride_date",
        to_date(col("pickup_timestamp"))
    )
)

# ---------------------------------------------------------
# Write incremental records
# ---------------------------------------------------------

incremental_output = "output/incremental/rides"

(
    processed_df.write
    .mode("append")
    .partitionBy("ride_date")
    .parquet(incremental_output)
)

# ---------------------------------------------------------
# Calculate next watermark
# ---------------------------------------------------------

next_watermark = (
    processed_df
    .agg({"last_updated": "max"})
    .collect()[0][0]
)

print("Previous watermark:", last_watermark)
print("New watermark:", next_watermark)
print("Incremental processing completed successfully.")
