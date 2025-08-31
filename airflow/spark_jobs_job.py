#!/usr/bin/env python3
"""
Jobs: Process and deduplicate monthly job data
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import sys

def main():
    # Get execution date from Airflow context or use current date
    if len(sys.argv) > 1:
        execution_date = sys.argv[1]
        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
    else:
        year = datetime.now().strftime("%Y")
        month = datetime.now().strftime("%m")

    # --- Spark Session ---
    spark = SparkSession.builder \
        .appName('adzuna_jobs') \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    # --- Path to Monthly Folder ---
    input_path = f"s3a://naya-project-job-ads/data/final/adzuna/{year}/{month}/*/*.parquet"

    # --- Read All Files in the Month ---
    df = spark.read.parquet(input_path)

    # --- Show original count ---
    original_count = df.count()
    print(f"Original count: {original_count}")

    # --- Drop Duplicate Job IDs ---
    df_unique = df.dropDuplicates(["id"])

    # --- Rename 'id' to 'job_id' ---
    df_unique = df_unique.withColumnRenamed("id", "job_id")

    # --- Show unique count ---
    unique_count = df_unique.count()
    print(f"Unique job ID count: {unique_count}")

    # --- Save monthly jobs to S3 in Parquet format ---
    output_path = f"s3a://naya-project-job-ads/data/jobs/{year}/{month}/"
    df_unique.write.mode("overwrite").parquet(output_path)

    print(f"✅ Monthly jobs saved to: {output_path}")
    spark.stop()

if __name__ == "__main__":
    main()