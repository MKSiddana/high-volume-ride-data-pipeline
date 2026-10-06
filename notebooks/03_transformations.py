from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    trim,
    round,
    unix_timestamp,
    to_date
)

spark = (
    SparkSession.builder
    .appName("RideDataTransformation")
    .getOrCreate()
)

raw_path = "output/raw/rides"

rides_df = spark.read.parquet(raw_path)

# ---------------------------------------------------------
# Clean and standardize
# ---------------------------------------------------------

clean_df = (
    rides_df
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
)

# ---------------------------------------------------------
# Calculate ride duration
# ---------------------------------------------------------

clean_df = clean_df.withColumn(
    "ride_duration_minutes",
    round(
        (
            unix_timestamp("dropoff_timestamp")
            - unix_timestamp("pickup_timestamp")
        ) / 60,
        2
    )
)

# ---------------------------------------------------------
# Add partition column
# ---------------------------------------------------------

clean_df = clean_df.withColumn(
    "ride_date",
    to_date(col("pickup_timestamp"))
)

# ---------------------------------------------------------
# Filter valid records
# ---------------------------------------------------------

valid_statuses = ["COMPLETED", "CANCELLED"]

curated_df = clean_df.filter(
    col("ride_id").isNotNull()
    & col("ride_status").isin(valid_statuses)
)

# ---------------------------------------------------------
# Repartition for scalable processing
# ---------------------------------------------------------

optimized_df = curated_df.repartition(
    "ride_date"
)

# ---------------------------------------------------------
# Write curated dataset
# ---------------------------------------------------------

curated_path = "output/curated/rides"

(
    optimized_df.write
    .mode("overwrite")
    .partitionBy("ride_date")
    .parquet(curated_path)
)

print("Ride transformation completed successfully.")
print("Curated ride count:", optimized_df.count())

optimized_df.show(truncate=False)
