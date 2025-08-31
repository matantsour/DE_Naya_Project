from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
)
from datetime import datetime
year = datetime.now().strftime("%Y")
month = datetime.now().strftime("%m")
day = datetime.now().strftime("%d")
# --- Spark Session ---
spark = SparkSession.builder \
    .appName('adzuna_stage_1') \
    .getOrCreate()

# --- Schema ---
jobs_schema = StructType([
    StructField("id", StringType(), True),
    StructField("created", TimestampType(), True),  # will convert later to Timestamp
    StructField("location_display_name", StringType(), True),
    StructField("location_Country", StringType(), True),
    StructField("location_State", StringType(), True),
    StructField("location_County", StringType(), True),
    StructField("location_City", StringType(), True),
    StructField("latitude", StringType(), True),   # will convert to Double
    StructField("longitude", StringType(), True),  # will convert to Double
    StructField("contract_time", StringType(), True),
    StructField("contract_type", StringType(), True),
    StructField("category_tag", StringType(), True),
    StructField("category_label", StringType(), True),
    StructField("company_display_name", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("salary_is_predicted", StringType(), True),  # will convert to Boolean
    StructField("salary_min", StringType(), True),           # will convert to Double
    StructField("salary_max", StringType(), True),           # will convert to Double
    StructField("redirect_url", StringType(), True),
])
# --- Read from S3 with schema ---
df = spark.read.schema(jobs_schema).json(f"s3a://naya-project-job-ads/data/raw/adzuna/{year}/{month}/{day}/")
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
    'location_display_name',
    'country',
    'state',
    'county',
    'city',
    'contract_time',
    'contract_type',
    'category_tag',
    'category_label',
    'company_name',
    'title',
    'description',
    'redirect_url'
]
for col_name in cols_to_string:
    df = df.withColumn(col_name, F.col(col_name).cast(StringType()))

# --- Cache for later use ---
df.cache()
df.printSchema()
output_path = f"s3a://naya-project-job-ads/data/stages/adzuna/stage_1/{year}/{month}/{day}/"
df.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"Data saved to {output_path} in Parquet format.")