from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
)

# --- Spark Session ---
spark = SparkSession.builder \
    .appName('adzuna_stage_4') \
    .getOrCreate()

from datetime import datetime
year = datetime.now().strftime("%Y")
month = datetime.now().strftime("%m")
day = datetime.now().strftime("%d")


path = f"s3a://naya-project-job-ads/data/stages/adzuna/stage_3/{year}/{month}/{day}/"
df = spark.read.parquet(path)

# --- Add normally distributed bid ---
mean = 3.25
stddev = 1.0
min_bid = 0.25
max_bid = 6.25

# Generate random value once per row
df = df.withColumn("rand_val", F.randn() * stddev + mean)

# Clamp to range [0.25, 6.25]
df = df.withColumn(
    "bid_raw",
    F.when(F.col("rand_val") < min_bid, min_bid)
     .when(F.col("rand_val") > max_bid, max_bid)
     .otherwise(F.col("rand_val"))
)

# Round UP to next multiple of 0.25
df = df.withColumn(
    "bid",
    (F.ceil(F.col("bid_raw") / F.lit(0.25)) * 0.25).cast(DoubleType())
)

# Drop helper columns
df = df.drop("rand_val", "bid_raw")

# --- Save to final folder---
path = f"s3a://naya-project-job-ads/data/final/adzuna/{year}/{month}/{day}/"
df.write.mode("overwrite").parquet(path)

print("✅ Stage 4 - Final data - completed successfully with bid in 0.25 steps.")
