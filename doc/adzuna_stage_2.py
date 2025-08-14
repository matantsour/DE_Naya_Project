#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
)


# In[2]:


def get_missing_locations_longitude_latitude_dict(missing_locations_longitude_latitude):
    # Dictionary to store results
    missing_locations_longitude_latitude_dict = {}
    
    # Nominatim requires a custom User-Agent
    headers = {
        "User-Agent": "MyReverseGeocoder/1.0"
    }
    
    for lon, lat in missing_locations_longitude_latitude:
        url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json"
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                results = response.json()
                results = {'city': results['address'].get('city',results['address'].get('town',results['address'].get('road'))),\
                            'county': results['address']['county'],\
                            'state': results['address']['state'],\
                            'country': results['address']['country_code'].upper()}
                missing_locations_longitude_latitude_dict[(lon, lat)] = results
            else:
                print(f"Failed for {(lon, lat)}: {response.status_code}")
                missing_locations_longitude_latitude_dict[(lon, lat)] = None
        except Exception as e:
            print(f"Error for {(lon, lat)}: {e}\n{url}")
            missing_locations_longitude_latitude_dict[(lon, lat)] = None
        
        time.sleep(1)
    
    return missing_locations_longitude_latitude_dict
        


# In[3]:


spark = SparkSession.builder \
    .appName('adzuna_stage_2') \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
jobs = spark.read.parquet("s3a://naya-project-job-ads/data/stages/adzuna/stage_1/2025/08/14/")
jobs.printSchema()


# In[4]:


#Check if duplicate ids exists:
n = jobs.count()
n_1 = jobs.dropDuplicates(["id"]).count()
if n>n_1:
    print(f"some duplicate ids exists: {n-n_1} duplicate ids\nRemoving duplicate ids")
    jobs = jobs.dropDuplicates(["id"])

n_null_created = jobs.filter(F.col("created").isNull()).count()
if n_null_created>0:
    window_spec = Window.orderBy("created").rowsBetween(Window.unboundedPreceding, 0)
    jobs = jobs.withColumn(
    "created",
    F.last("created", ignorenulls=True).over(window_spec)
)
jobs = jobs.withColumn('created',F.col('created').cast(StringType()))
jobs = jobs.filter(~(F.col("latitude").isNull() | F.col("longitude").isNull()))


# In[ ]:





# In[5]:


pdf = jobs.toPandas()


# In[6]:


import pandas as pd
l= ['id', 'created', 'location_display_name', 'country', 'state', 'county',
       'city', 'latitude', 'longitude', 'contract_time', 'contract_type',
       'category_tag', 'category_label', 'company_name', 'title',
       'description', 'salary_is_predicted', 'salary_min', 'salary_max',
       'redirect_url']
for col in l:
    if pd.isna(pdf[col]).sum()>0:
        print(col)


# In[ ]:





# In[ ]:





# In[7]:


missing_locations_mask = (pdf['city'].isna()) |( pdf['county'].isna()) | (pdf['state'].isna()) |( pdf['country'].isna())
missing_locations_longitude_latitude = pdf.loc[missing_locations_mask,['longitude','latitude']].drop_duplicates().to_records(index=False).tolist()
get_missing_locations_longitude_latitude_dict = get_missing_locations_longitude_latitude_dict(missing_locations_longitude_latitude)
city_col = pdf.loc[missing_locations_mask ,['longitude','latitude']].apply(lambda X: get_missing_locations_longitude_latitude_dict.get((X['longitude'],X['latitude'])).get('city'),axis=1)
county_col = pdf.loc[missing_locations_mask ,['longitude','latitude']].apply(lambda X: get_missing_locations_longitude_latitude_dict.get((X['longitude'],X['latitude'])).get('county'),axis=1)
state_col = pdf.loc[missing_locations_mask ,['longitude','latitude']].apply(lambda X: get_missing_locations_longitude_latitude_dict.get((X['longitude'],X['latitude'])).get('state'),axis=1)
country_col = pdf.loc[missing_locations_mask,['longitude','latitude']].apply(lambda X: get_missing_locations_longitude_latitude_dict.get((X['longitude'],X['latitude'])).get('country'),axis=1)


# In[9]:


pdf.loc[missing_locations_mask ,'city'],pdf.loc[missing_locations_mask ,'county'],pdf.loc[missing_locations_mask ,'state'],pdf.loc[missing_locations_mask ,'country'] =\
city_col,county_col,state_col,country_col

