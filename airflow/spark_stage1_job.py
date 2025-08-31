#!/usr/bin/env python3
"""
Adzuna Stage 1: Data ingestion and schema enforcement
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
        .appName('adzuna_stage_1') \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    # --- Schema ---
    jobs_schema = StructType([
        StructField("id", StringType(), True),
        StructField("created", TimestampType(), True),
        StructField("location_display_name", StringType(), True),
        StructField("location_Country", StringType(), True),
        StructField("location_State", StringType(), True),
        StructField("location_County", StringType(), True),
        StructField("location_City", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("contract_time", StringType(), True),
        StructField("contract_type", StringType(), True),
        StructField("category_tag", StringType(), True),
        StructField("category_label", StringType(), True),
        StructField("company_display_name", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("salary_is_predicted", StringType(), True),
        StructField("salary_min", StringType(), True),
        StructField("salary_max", StringType(), True),
        StructField("redirect_url", StringType(), True),
    ])

    # --- Read from S3 with schema ---
    input_path = f"s3a://naya-project-job-ads/data/raw/adzuna/{year}/{month}/{day}/"
    df = spark.read.schema(jobs_schema).json(input_path)

    # --- Rename columns ---
    df = df.withColumnRenamed("location_Country", "country") \
           .withColumnRenamed("location_State", "state") \
           .withColumnRenamed("location_County", "county") \
           .withColumnRenamed("location_City", "city") \
           .withColumnRenamed("company_display_name", "company_name")

    # --- Cast data types ---
    df = df.withColumn("created", F.to_timestamp("created")) \
           .withColumn("latitude", F.col("latitude").cast(DoubleType())) \
           .withColumn("longitude", F.col("longitude").cast(DoubleType())) \
           .withColumn("salary_min", F.col("salary_min").cast(DoubleType())) \
           .withColumn("salary_max", F.col("salary_max").cast(DoubleType())) \
           .withColumn("salary_is_predicted", (F.col("salary_is_predicted").cast("int") == 1))

    # --- Ensure certain columns are strings ---
    cols_to_string = [
        'location_display_name', 'country', 'state', 'county', 'city',
        'contract_time', 'contract_type', 'category_tag', 'category_label',
        'company_name', 'title', 'description', 'redirect_url'
    ]
    for col_name in cols_to_string:
        df = df.withColumn(col_name, F.col(col_name).cast(StringType()))

    # --- Cache for performance ---
    df.cache()
    
    # --- Save to stage 1 ---
    output_path = f"s3a://naya-project-job-ads/data/stages/adzuna/stage_1/{year}/{month}/{day}/"
    df.write.mode("overwrite").parquet(output_path)

    print(f"✅ Stage 1 completed. Data saved to {output_path}")
    spark.stop()

if __name__ == "__main__":
    main()