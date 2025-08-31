import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
)
from datetime import datetime
year = datetime.now().strftime("%Y")
month = datetime.now().strftime("%m")
day = datetime.now().strftime("%d")

# --- Function to get locations from Nominatim ---
def get_missing_locations_longitude_latitude_dict(missing_locations_longitude_latitude):
    missing_dict = {}
    headers = {"User-Agent": "MyReverseGeocoder/1.0"}

    for lon, lat in missing_locations_longitude_latitude:
        url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json"
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                address = data.get('address', {})
                results = {
                    'city': address.get('city') or address.get('town') or address.get('road'),
                    'county': address.get('county'),
                    'state': address.get('state'),
                    'country': address.get('country_code').upper() if address.get('country_code') else None
                }
                missing_dict[(lon, lat)] = results
            else:
                missing_dict[(lon, lat)] = None
        except Exception as e:
            print(f"Error for {(lon, lat)}: {e}\n{url}")
            missing_dict[(lon, lat)] = None
        time.sleep(1)
    return missing_dict


# --- Start Spark ---
spark = SparkSession.builder \
    .appName('adzuna_stage_2') \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

jobs = spark.read.parquet(f"s3a://naya-project-job-ads/data/stages/adzuna/stage_1/{year}/{month}/{day}/")
jobs.printSchema()

# --- Drop duplicate ids ---
jobs = jobs.dropDuplicates(["id"])

# --- Fill null 'created' with last known value ---
window_spec = Window.orderBy("created").rowsBetween(Window.unboundedPreceding, 0)
jobs = jobs.withColumn("created", F.last("created", ignorenulls=True).over(window_spec))
jobs = jobs.withColumn('created', F.col('created').cast(StringType()))

# --- Remove rows with null latitude or longitude ---
jobs = jobs.filter(~(F.col("latitude").isNull() | F.col("longitude").isNull()))

# --- Identify missing locations ---
missing_df = jobs.filter(
    F.col('city').isNull() | F.col('county').isNull() | F.col('state').isNull() | F.col('country').isNull()
).select('longitude', 'latitude').distinct()

# --- Collect missing coordinates to driver ---
missing_coords = [(row['longitude'], row['latitude']) for row in missing_df.collect()]

# --- Get missing locations from Nominatim ---
missing_locations_dict = get_missing_locations_longitude_latitude_dict(missing_coords)

# --- Broadcast the dictionary for Spark ---
broadcast_missing = spark.sparkContext.broadcast(missing_locations_dict)

# --- UDF to map missing locations ---
def map_missing_location(lon, lat, col_name):
    val = broadcast_missing.value.get((lon, lat))
    if val:
        return val.get(col_name)
    else:
        return None

map_missing_udf = F.udf(map_missing_location, StringType())

# --- Fill missing columns using UDF ---
for col_name in ['city', 'county', 'state', 'country']:
    jobs = jobs.withColumn(
        col_name,
        F.when(
            F.col(col_name).isNull(),
            map_missing_udf(F.col('longitude'), F.col('latitude'), F.lit(col_name))
        ).otherwise(F.col(col_name))
    )

# --- Save final dataframe as parquet ---
jobs.write.mode("overwrite").parquet(
    f"s3a://naya-project-job-ads/data/stages/adzuna/stage_2/{year}/{month}/{day}/"
)

print("✅ Stage 2 completed successfully.")
