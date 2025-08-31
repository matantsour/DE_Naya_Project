#!/usr/bin/env python3
"""
Adzuna Stage 3: Data transformations and feature engineering
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
)
from datetime import datetime
import sys

def main():
    # Get execution date from Airflow context or use current date
    if len(sys.argv) > 1:
        execution_date = sys.argv[1]
        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
    else:
        year = datetime.now().strftime("%Y")
        month = datetime.now().strftime("%m")
        day = datetime.now().strftime("%d")

    # --- Spark Session ---
    spark = SparkSession.builder \
        .appName('adzuna_stage_3') \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    # --- Load Stage 2 data ---
    input_path = f"s3a://naya-project-job-ads/data/stages/adzuna/stage_2/{year}/{month}/{day}/"
    df = spark.read.parquet(input_path)
    
    print(f"Loaded {df.count()} records from stage 2")

    # --- Transformations ---

    # 1. Break up created timestamp into parts
    df = df.withColumn("created_ts", F.to_timestamp("created")) \
           .withColumn("created_date", F.to_date("created_ts")) \
           .withColumn("created_hour", F.hour("created_ts")) \
           .withColumn(
               "created_time_of_day",
               F.when((F.col("created_hour") >= 5) & (F.col("created_hour") < 12), "morning")
                .when((F.col("created_hour") >= 12) & (F.col("created_hour") < 17), "afternoon")
                .when((F.col("created_hour") >= 17) & (F.col("created_hour") < 21), "evening")
                .otherwise("night")
           )

    # 2. Fill nulls in contract_time with default
    df = df.fillna({"contract_time": "full_time"})

    # 3. Drop contract_type column
    df = df.drop("contract_type")

    # 4. Drop category_tag, keep category_label
    df = df.drop("category_tag")

    # 5. Description: detect "bonus" or "benefit" keywords
    df = df.withColumn(
        "has_bonus_or_benefit",
        F.when(
            F.lower(F.col("description")).rlike("bonus|benefit"),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    # --- Save to stage 3 ---
    output_path = f"s3a://naya-project-job-ads/data/stages/adzuna/stage_3/{year}/{month}/{day}/"
    df.write.mode("overwrite").parquet(output_path)

    print(f"✅ Stage 3 completed successfully. Data saved to {output_path}")
    spark.stop()

if __name__ == "__main__":
    main()