from datetime import datetime, timedelta
import requests
import json
import boto3
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'adzuna_job_data_pipeline',
    default_args=default_args,
    description='Complete Adzuna job data extraction and processing pipeline',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['adzuna', 'job-data', 'etl']
)

# =====================
# EXTRACTION FUNCTIONS
# =====================

def get_job_location_area(job_location_area_list):
    """Extract location information from job location area list"""
    if not job_location_area_list:
        return {}
    if len(job_location_area_list) != 4:
        return {}
    return {
        'Country': job_location_area_list[0],
        'State': job_location_area_list[1], 
        'County': job_location_area_list[2],
        'City': job_location_area_list[3]
    }

def flatten_job(job):
    """Flatten nested job structure"""
    area_dict = get_job_location_area(job.get("location", {}).get("area"))
    return {
        "title": job.get("title"),
        "salary_max": job.get("salary_max"),
        "salary_min": job.get("salary_min"),
        "salary_is_predicted": job.get("salary_is_predicted"),
        "description": job.get("description"),
        "redirect_url": job.get("redirect_url"),
        "id": job.get("id"),
        "created": job.get("created"),
        'contract_type': job.get('contract_type'),
        'contract_time': job.get('contract_time'),
        # Flatten category
        "category_label": job.get("category", {}).get("label"),
        "category_tag": job.get("category", {}).get("tag"),
        # Flatten company
        "company_display_name": job.get("company", {}).get("display_name"),
        # Flatten location
        "location_display_name": job.get("location", {}).get("display_name"),
        'location_Country': area_dict.get('Country'),
        'location_State': area_dict.get('State'),
        'location_County': area_dict.get('County'),
        'location_City': area_dict.get('City'),
        'latitude': job.get('latitude'),
        'longitude': job.get('longitude'),
    }

def upload_results_to_aws_bucket(results, job_title, state, city, set_name):
    """Upload results to S3 bucket"""
    bucket_name = 'naya-project-job-ads'
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    day = datetime.now().strftime("%d")

    key = f'data/raw/adzuna/{year}/{month}/{day}/{timestamp}_{set_name}.json'
    flattened_jobs = [flatten_job(job) for job in results.get("results", [])]
    json_lines = "\n".join(json.dumps(job) for job in flattened_jobs)

    s3 = boto3.client('s3')
    
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json_lines,
            ContentType='application/json'
        )
        print(f"✅ Uploaded {len(flattened_jobs)} flattened jobs: {job_title} in {city}, {state}")
        return True
    except Exception as e:
        print(f"❌ S3 Upload failed for {job_title} in {city}, {state}: {e}")
        raise

def extract_from_adzuna(job_title, state, city, app_id, app_key):
    """Extract job data from Adzuna API"""
    country = 'us'
    endpoint = f'https://api.adzuna.com/v1/api/jobs/{country}/search/1'
    params = {
        'app_id': app_id,
        'app_key': app_key,
        'what': job_title,
        'where': f'{city}, {state}',
        'results_per_page': 20,
        'sort_by': 'date'
    }

    try:
        response = requests.get(endpoint, params=params)
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            if results:
                return data
            else:
                print(f"⚠️ No results for {job_title} in {city}, {state}")
                return None
        else:
            print(f"❌ API error {response.status_code} for {job_title} in {city}, {state}")
            raise Exception(f"API error {response.status_code}")
    except Exception as e:
        print(f"❌ Request failed for {job_title} in {city}, {state}: {e}")
        raise

def run_adzuna_extraction_set1(**context):
    """Run Adzuna extraction for set 1 (Matan's credentials)"""
    APP_ID = '223d9f60'
    APP_KEY = '3f4c7510782d76d9edffe71e90119545'
    
    job_titles = [
        "Product Manager",
        "DevOps Engineer", 
        "Machine Learning Engineer",
        "Sales Representative",
        "Content Writer",
    ]

    states = {
        "California": ["Los Angeles", "San Diego", "San Jose"],
        "Texas": ["Houston", "San Antonio", "Dallas"],
        "Florida": ["Jacksonville", "Miami", "Tampa"],
        "New York": ["New York City", "Buffalo", "Rochester"],
        "Illinois": ["Chicago", "Aurora", "Naperville"],
    }

    search_list = [
        {"Job_Title": jt, "State": state, "City": city}
        for jt in job_titles
        for state, cities in states.items()
        for city in cities
    ]

    for i, search in enumerate(search_list, 1):
        job_title = search["Job_Title"]
        state = search["State"]
        city = search["City"]
        print(f"\n🔍 ({i}/{len(search_list)}) Searching: {job_title} in {city}, {state}")

        data = extract_from_adzuna(job_title, state, city, APP_ID, APP_KEY)
        if data:
            upload_results_to_aws_bucket(data, job_title, state, city, "set1")

        if i < len(search_list):
            print("⏳ Waiting 30 seconds to avoid rate limits...\n")
            time.sleep(30)

    print("\n✅ Set 1 extraction completed.")

def run_adzuna_extraction_set2(**context):
    """Run Adzuna extraction for set 2 (Avital's credentials)"""
    APP_ID = '85001579'
    APP_KEY = '3f42741594f1135c2c9a6e2f601b509a'
    
    job_titles = [
        "Product Manager",
        "DevOps Engineer",
        "Machine Learning Engineer", 
        "Sales Representative",
        "Content Writer",
    ]

    states = {
        "California": ["Los Angeles", "San Diego", "San Jose"],
        "Texas": ["Houston", "San Antonio", "Dallas"],
        "Florida": ["Jacksonville", "Miami", "Tampa"],
        "New York": ["New York City", "Buffalo", "Rochester"],
        "Illinois": ["Chicago", "Aurora", "Naperville"],
    }

    search_list = [
        {"Job_Title": jt, "State": state, "City": city}
        for jt in job_titles
        for state, cities in states.items()
        for city in cities
    ]

    for i, search in enumerate(search_list, 1):
        job_title = search["Job_Title"]
        state = search["State"]
        city = search["City"]
        print(f"\n🔍 ({i}/{len(search_list)}) Searching: {job_title} in {city}, {state}")

        data = extract_from_adzuna(job_title, state, city, APP_ID, APP_KEY)
        if data:
            upload_results_to_aws_bucket(data, job_title, state, city, "set2")

        if i < len(search_list):
            print("⏳ Waiting 30 seconds to avoid rate limits...\n")
            time.sleep(30)

    print("\n✅ Set 2 extraction completed.")

# =====================
# AIRFLOW TASKS
# =====================

# Task 1a: Extract data using Matan's credentials
extract_set1_task = PythonOperator(
    task_id='extract_adzuna_set1',
    python_callable=run_adzuna_extraction_set1,
    dag=dag,
    pool='extraction_pool'
)

# Task 1b: Extract data using Avital's credentials  
extract_set2_task = PythonOperator(
    task_id='extract_adzuna_set2',
    python_callable=run_adzuna_extraction_set2,
    dag=dag,
    pool='extraction_pool'
)

# Task 2: Stage 1 - Data ingestion and schema enforcement
stage1_task = SparkSubmitOperator(
    task_id='adzuna_stage_1',
    application='/opt/airflow/spark_jobs/adzuna_stage_1.py',
    conn_id='spark_default',
    application_args=[],
    dag=dag,
    executor_memory='2g',
    driver_memory='1g'
)

# Task 3: Stage 2 - Geocoding and location enrichment
stage2_task = SparkSubmitOperator(
    task_id='adzuna_stage_2', 
    application='/opt/airflow/spark_jobs/adzuna_stage_2.py',
    conn_id='spark_default',
    application_args=[],
    dag=dag,
    executor_memory='4g',
    driver_memory='2g'
)

# Task 4: Stage 3 - Data transformations
stage3_task = SparkSubmitOperator(
    task_id='adzuna_stage_3',
    application='/opt/airflow/spark_jobs/adzuna_stage_3.py', 
    conn_id='spark_default',
    application_args=[],
    dag=dag,
    executor_memory='2g',
    driver_memory='1g'
)

# Task 5: Stage 4 - Add bid data and finalize
stage4_task = SparkSubmitOperator(
    task_id='adzuna_stage_4',
    application='/opt/airflow/spark_jobs/adzuna_stage_4.py',
    conn_id='spark_default', 
    application_args=[],
    dag=dag,
    executor_memory='2g',
    driver_memory='1g'
)

# Task 6a: Generate campaigns
campaigns_task = SparkSubmitOperator(
    task_id='generate_campaigns',
    application='/opt/airflow/spark_jobs/campaigns.py',
    conn_id='spark_default',
    application_args=[],
    dag=dag,
    executor_memory='2g',
    driver_memory='1g'
)

# Task 6b: Process jobs
jobs_task = SparkSubmitOperator(
    task_id='process_jobs',
    application='/opt/airflow/spark_jobs/jobs.py',
    conn_id='spark_default',
    application_args=[],
    dag=dag,
    executor_memory='2g', 
    driver_memory='1g'
)

# Task 7: Generate clicks data
clicks_task = SparkSubmitOperator(
    task_id='generate_clicks',
    application='/opt/airflow/spark_jobs/clicks.py',
    conn_id='spark_default',
    application_args=[],
    dag=dag,
    executor_memory='4g',
    driver_memory='2g'
)

# =====================
# TASK DEPENDENCIES  
# =====================

# Stage 1: Parallel extraction
[extract_set1_task, extract_set2_task] >> stage1_task

# Sequential processing stages
stage1_task >> stage2_task >> stage3_task >> stage4_task

# Parallel final processing
stage4_task >> [campaigns_task, jobs_task]

# Final clicks generation depends on jobs
jobs_task >> clicks_task