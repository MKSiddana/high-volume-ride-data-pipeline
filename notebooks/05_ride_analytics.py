from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    round
)

spark = (
    SparkSession.builder
    .appName("RideAnalytics")
    .getOrCreate()
)

curated_path = "output/curated/rides"

rides_df = spark.read.parquet(curated_path)

# Create temporary view for Spark SQL
rides_df.createOrReplaceTempView("rides")

# ---------------------------------------------------------
# City-level ride analytics
# ---------------------------------------------------------

city_summary = spark.sql("""
    SELECT
        city,
        COUNT(ride_id) AS total_rides,
        ROUND(SUM(fare_amount), 2) AS total_revenue,
        ROUND(AVG(fare_amount), 2) AS average_fare,
        ROUND(AVG(distance_km), 2) AS average_distance_km
    FROM rides
    WHERE ride_status = 'COMPLETED'
    GROUP BY city
    ORDER BY total_revenue DESC
""")

city_summary.show(truncate=False)

# ---------------------------------------------------------
# Payment method analytics
# ---------------------------------------------------------

payment_summary = (
    rides_df
    .filter("ride_status = 'COMPLETED'")
    .groupBy("payment_method")
    .agg(
        count("ride_id").alias("total_rides"),
        round(sum("fare_amount"), 2).alias("total_revenue"),
        round(avg("fare_amount"), 2).alias("average_fare")
    )
)

payment_summary.show(truncate=False)

# ---------------------------------------------------------
# Write analytics output
# ---------------------------------------------------------

analytics_path = "output/analytics/city_summary"

(
    city_summary.write
    .mode("overwrite")
    .parquet(analytics_path)
)

print("Ride analytics generated successfully.")
