from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

# Initialize SparkSession
spark = SparkSession.builder.appName("GenerateClicks").getOrCreate()

year = "2025"
month = "08"

# Load jobs with necessary fields (including created_date and bid)
jobs_path = f"s3a://naya-project-job-ads/data/jobs/{year}/{month}/*.parquet"
jobs_df = spark.read.parquet(jobs_path).select(
    "job_id", "created_date", "bid"
).filter(F.col("created_date").isNotNull())

# Get min and max dates
min_date = jobs_df.select(F.min("created_date")).first()[0]
max_date = jobs_df.select(F.max("created_date")).first()[0]

# Create a DataFrame with a full date range
date_range_df = (
    spark.range(1)  # dummy row
    .withColumn("date_seq", F.expr(f"sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)"))
    .withColumn("date", F.explode("date_seq"))
    .drop("date_seq")
)

# Join jobs with date_range filtering dates after created_date
jobs_dates = jobs_df.crossJoin(date_range_df) \
    .filter(F.col("date") >= F.col("created_date"))

# For each job_id and date, generate random clicks count between 10-80
clicks_df = jobs_dates.withColumn(
    "num_clicks",
    (F.floor(F.rand() * 71 + 10)).cast("int")  # 10 to 80 clicks
)

# Generate a sequence [1 .. num_clicks] to explode
clicks_df = clicks_df.withColumn("click_idx", F.explode(F.sequence(F.lit(1), F.col("num_clicks"))))

# Add unique click_id using job_id, date, and click_idx
clicks_df = clicks_df.withColumn(
    "click_id",
    F.concat_ws("_", F.col("job_id"), F.col("date").cast(StringType()), F.col("click_idx"))
)

# cost ~ N(mean=bid, std=0.5)
clicks_df = clicks_df.withColumn(
    "cost",
    F.col("bid") + F.randn() * 0.5
)

# Add random device_type: 'Mobile' or 'PC'
clicks_df = clicks_df.withColumn(
    "device_type",
    F.when(F.rand() < 0.5, "Mobile").otherwise("PC")
)

# Add random times (HH:MM:SS)
clicks_df = clicks_df.withColumn(
    "time",
    F.concat(
        F.lpad(F.floor(F.rand() * 24).cast("int").cast("string"), 2, "0"), F.lit(":"),
        F.lpad(F.floor(F.rand() * 60).cast("int").cast("string"), 2, "0"), F.lit(":"),
        F.lpad(F.floor(F.rand() * 60).cast("int").cast("string"), 2, "0")
    )
)

# Add gender randomly Male/Female
clicks_df = clicks_df.withColumn(
    "gender",
    F.when(F.rand() < 0.5, "Male").otherwise("Female")
)

# Add age_group randomly from list
age_groups = ["18-25", "25-32", "32-40", "40-50", "50-60", "60-99"]
age_group_array = "array(" + ",".join([f"'{ag}'" for ag in age_groups]) + ")"

clicks_df = clicks_df.withColumn(
    "age_group",
    F.expr(f"shuffle({age_group_array})[0]")
)

# Add is_applicant using normal approx p ~ N(0.15, 0.03)
# applicant_prob ~ N(mean=0.15, std=0.03)
clicks_df = clicks_df.withColumn(
    "applicant_prob",
    F.randn() * 0.03 + F.lit(0.15)
)

# Clip to [0,1]
clicks_df = clicks_df.withColumn(
    "applicant_prob_clipped",
    F.when(F.col("applicant_prob") < 0, 0)
    .when(F.col("applicant_prob") > 1, 1)
    .otherwise(F.col("applicant_prob"))
)

# Generate is_applicant as 1 if rand() < applicant_prob_clipped else 0
clicks_df = clicks_df.withColumn(
    "is_applicant",
    F.when(F.rand() < F.col("applicant_prob_clipped"), 1).otherwise(0)
)

# Drop helper columns
clicks_df = clicks_df.drop("applicant_prob", "applicant_prob_clipped", "click_idx", "num_clicks")

# Final select and write to your preferred path
final_clicks = clicks_df.select(
    "click_id", "job_id", "date", "time", "cost",
    "device_type", "gender", "age_group", "is_applicant"
)

output_path = f"s3a://naya-project-job-ads/data/clicks/{year}/{month}/"
final_clicks.write.mode("overwrite").parquet(output_path)

print(f"✅ Click data written to {output_path}")
