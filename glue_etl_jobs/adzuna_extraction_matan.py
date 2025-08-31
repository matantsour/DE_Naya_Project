import requests
import json
import boto3
import time
from datetime import datetime

# Your Adzuna credentials
APP_ID = '223d9f60'
APP_KEY = '3f4c7510782d76d9edffe71e90119545'

# --- Upload to S3 ---
def get_job_location_area(job_location_area_list):
    if not job_location_area_list:
        return {}
    if len(job_location_area_list)!=4:
        return {}
    return {'Country':job_location_area_list[0],'State':job_location_area_list[1],'County':job_location_area_list[2],'City':job_location_area_list[3]}

def flatten_job(job):
    # Flatten nested fields category, company, location with safe defaults
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
        'location_Country':area_dict.get('Country'),
        'location_State': area_dict.get('State'),
        'location_County': area_dict.get('County'),
        'location_City': area_dict.get('City'),
        'latitude': job.get('latitude'),
        'longitude': job.get('longitude'),
    }

def upload_results_to_aws_bucket(results, job_title, state, city):
    bucket_name = 'naya-project-job-ads'
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    day = datetime.now().strftime("%d")

    key = f'data/raw/adzuna/{year}/{month}/{day}/{timestamp}_set1.json'
    flattened_jobs = [flatten_job(job) for job in results.get("results", [])]
    # Convert each dict to JSON line
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
    except Exception as e:
        print(f"❌ S3 Upload failed for {job_title} in {city}, {state}: {e}")

# --- API Call ---
def extract_from_adzuna(job_title, state, city):
    country = 'us'
    endpoint = f'https://api.adzuna.com/v1/api/jobs/{country}/search/1'
    params = {
        'app_id': APP_ID,
        'app_key': APP_KEY,
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
                upload_results_to_aws_bucket(data, job_title, state, city)
            else:
                print(f"⚠️ No results for {job_title} in {city}, {state}")
        else:
            print(f"❌ API error {response.status_code} for {job_title} in {city}, {state}")
    except Exception as e:
        print(f"❌ Request failed for {job_title} in {city}, {state}: {e}")

# --- Search List ---
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

def generate_search_list(job_titles, states_cities):
    return [
        {"Job_Title": jt, "State": state, "City": city}
        for jt in job_titles
        for state, cities in states_cities.items()
        for city in cities
    ]

# --- Main Loop ---
search_list = generate_search_list(job_titles, states)

for i, search in enumerate(search_list, 1):
    job_title = search["Job_Title"]
    state = search["State"]
    city = search["City"]
    print(f"\n🔍 ({i}/{len(search_list)}) Searching: {job_title} in {city}, {state}")

    extract_from_adzuna(job_title, state, city)

    if i < len(search_list):  # Don't sleep after last call
        print("⏳ Waiting 30 seconds to avoid rate limits...\n")
        time.sleep(30)

print("\n✅ Job completed.")