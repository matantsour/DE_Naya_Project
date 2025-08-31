from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

# --- Spark Session ---
spark = SparkSession.builder \
    .appName('adzuna_jobs') \
    .getOrCreate()

# --- Define Year and Month ---
year = "2025"
month = "08"

# --- Path to Monthly Folder ---
path = f"s3a://naya-project-job-ads/data/final/adzuna/{year}/{month}/*/*.parquet"

# --- Read All Files in the Month ---
df = spark.read.parquet(path)

# --- (Optional) Show original count ---
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
save_path = f"s3a://naya-project-job-ads/data/jobs/{year}/{month}/"
df_unique.write.mode("overwrite").parquet(save_path)

print(f"✅ Monthly jobs saved to: {save_path}")