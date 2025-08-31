from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from datetime import datetime

# --- Spark Session ---
spark = SparkSession.builder \
    .appName('adzuna_campaigns') \
    .getOrCreate()

# Params
year = "2025"
month = "08"

# --- Paths ---
monthly_campaigns_path = f"s3a://naya-project-job-ads/data/campaigns/{year}/{month}/"
final_ads_path = f"s3a://naya-project-job-ads/data/final/adzuna/{year}/{month}/*/*.parquet"

# --- Load old campaigns if exist ---
try:
    old_campaigns = spark.read.parquet(monthly_campaigns_path)
    old_campaigns = old_campaigns.cache()
    old_campaigns.count()  # force load, avoid lazy re-read
    max_id = old_campaigns.agg(F.max("campaign_id")).collect()[0][0] or 1000
    print(f"✅ Loaded existing campaigns. Max campaign_id = {max_id}")

except Exception:
    print("⚠️ No previous campaigns found, starting fresh")
    old_campaigns = None
    max_id = 1000

# --- Load final job ads for the month ---
df = spark.read.parquet(final_ads_path)

# --- Aggregate per campaign (company, state) ---
agg = df.groupBy("company_name", "state").agg(F.avg("bid").alias("avg_bid"))

# --- Determine NEW keys (company_name, state) NOT in old monthly campaigns ---
if old_campaigns is not None:
    new_keys = agg.join(
        old_campaigns.select("company_name", "state").distinct(),
        on=["company_name", "state"],
        how="left_anti"
    )
    # Keep ALL previously saved rows as-is; we won't drop/update them
    existing_all = old_campaigns.select(
        F.col("campaign_id").cast("long").alias("campaign_id"),
        F.col("company_name"),
        F.col("state"),
        F.col("cpa_target").cast("int").alias("cpa_target"),
        F.col("budget").cast("int").alias("budget"),
        F.col("margin").cast("double").alias("margin")
    )
else:
    new_keys = agg
    existing_all = None

# --- Enrich NEW campaigns only ---
if new_keys is not None and new_keys.limit(1).count() > 0:
    # Assign contiguous ids after current max_id
    # (row_number over a stable order; MIID could produce gaps)
    w = Window.orderBy(F.col("company_name"), F.col("state"))
    new_enriched = (
        new_keys
        .withColumn("seq", F.row_number().over(w))
        .withColumn("campaign_id", (F.col("seq") + F.lit(max_id)).cast("long"))
        .drop("seq")
    )

    # CPA target ~ avg_bid * Normal(30, 3)
    new_enriched = new_enriched.withColumn("rand_factor", (F.randn() * 3 + 30))
    new_enriched = new_enriched.withColumn(
        "cpa_target",
        (F.col("avg_bid") * F.col("rand_factor")).cast(IntegerType())
    )

    # Budget (safe indexing to avoid out-of-bounds/nulls)
    budget_choices = [10000,12500,15000,17500,20000,22500,25000,30000]
    budget_choices = [int(x/2) for x in budget_choices]
    N = len(budget_choices)
    budget_array = ",".join(map(str, budget_choices))
    new_enriched = new_enriched.withColumn(
        "budget",
        F.expr(f"element_at(array({budget_array}), least(cast(floor(rand()*{N}) as int) + 1, {N}))")
    )

    # Margin
    margin_choices = [0.3, 0.35, 0.4, 0.45]
    M = len(margin_choices)
    margin_array = ",".join(map(str, margin_choices))
    new_enriched = new_enriched.withColumn(
        "margin",
        F.expr(f"element_at(array({margin_array}), least(cast(floor(rand()*{M}) as int) + 1, {M}))")
    )

    # Final select & type-align
    new_final = new_enriched.select(
        F.col("campaign_id").cast("long").alias("campaign_id"),
        F.col("company_name"),
        F.col("state"),
        F.col("cpa_target").cast("int").alias("cpa_target"),
        F.col("budget").cast("int").alias("budget"),
        F.col("margin").cast("double").alias("margin")
    )
else:
    new_final = None

# --- Merge: keep all old rows + append new rows ---
if existing_all is not None and new_final is not None:
    final_campaigns = existing_all.unionByName(new_final)
elif existing_all is not None:
    final_campaigns = existing_all
else:
    final_campaigns = new_final

# --- Safety: ensure no duplicate (company_name, state) with different IDs ---
# Keep the lowest campaign_id for any accidental duplicates
if final_campaigns is None:
    raise RuntimeError("No campaigns to write (both existing and new are empty).")

dedup_w = Window.partitionBy("company_name", "state").orderBy("campaign_id")
final_campaigns = (
    final_campaigns
    .withColumn("rn", F.row_number().over(dedup_w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# exclude_null_campaigns
final_campaigns = final_campaigns.filter(F.col("company_name").isNotNull())

# --- Save merged monthly campaigns ---
final_campaigns.write.mode("overwrite").parquet(monthly_campaigns_path)
print(f"✅ Final campaigns saved to {monthly_campaigns_path}")
