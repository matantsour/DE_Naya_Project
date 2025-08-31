#!/usr/bin/env python3
"""
Adzuna Stage 4: Add bid data and finalize
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
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
        .appName('adzuna_stage_4') \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    # --- Load Stage 3 data ---
    input_path = f"s3a://naya-project-job-ads/data/stages/adzuna/stage_3/{year}/{month}/{day}/"
    df = spark.read.parquet(input_path)
    
    print(f"Loaded {df.count()} records from stage 3")

    # --- Add normally distributed bid ---
    mean = 3.25
    stddev = 1.0
    min_bid = 0.25
    max_bid = 6.25

    # Generate random value once per row
    df = df.withColumn("rand_val", F.randn() * stddev + mean)

    # Clamp to range [0.25, 6.25]
    df = df.withColumn(
        "