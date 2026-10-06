from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = (
    SparkSession.builder
    .appName("RideDataQuality")
    .getOrCreate()
)

raw_path = "output/raw/rides"

rides_df = spark.read.parquet(raw_path)

# Duplicate ride check
duplicate_rides = (
    rides_df
    .groupBy("ride_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

# Required field validation
missing_required = rides_df.filter(
    col("ride_id").isNull()
    | col("driver_id").isNull()
    | col("rider_id").isNull()
    | col("pickup_timestamp").isNull()
)

# Completed rides must have valid distance and fare
invalid_completed_rides = rides_df.filter(
    (col("ride_status") == "COMPLETED")
    & (
        (col("distance_km") <= 0)
        | (col("fare_amount") <= 0)
        | col("dropoff_timestamp").isNull()
    )
)

# Drop-off cannot occur before pickup
invalid_timestamps = rides_df.filter(
    col("dropoff_timestamp") < col("pickup_timestamp")
)

# Validate allowed statuses
valid_statuses = ["COMPLETED", "CANCELLED"]

invalid_status = rides_df.filter(
    ~col("ride_status").isin(valid_statuses)
)

print("Total rides:", rides_df.count())
print("Duplicate ride IDs:", duplicate_rides.count())
print("Missing required fields:", missing_required.count())
print("Invalid completed rides:", invalid_completed_rides.count())
print("Invalid timestamps:", invalid_timestamps.count())
print("Invalid ride statuses:", invalid_status.count())

if duplicate_rides.count() > 0:
    raise ValueError("Data quality failure: duplicate ride IDs detected.")

if missing_required.count() > 0:
    raise ValueError("Data quality failure: required fields are missing.")

if invalid_completed_rides.count() > 0:
    raise ValueError("Data quality failure: invalid completed ride data.")

if invalid_timestamps.count() > 0:
    raise ValueError("Data quality failure: invalid ride timestamps.")

if invalid_status.count() > 0:
    raise ValueError("Data quality failure: invalid ride status detected.")

print("All ride data quality checks passed successfully.")
