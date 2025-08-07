import requests
import json
import boto3
import time
from datetime import datetime

# Your Adzuna credentials
APP_ID = '223d9f60'
APP_KEY = '3f4c7510782d76d9edffe71e90119545'


# --- Upload to S3 ---
def upload_results_to_aws_bucket(results, job_title, state, city):
    bucket_name = 'naya-project-job-ads'
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    key = f'data/raw/adzuna/{timestamp}.json'

    json_string = json.dumps(results, indent=2)
    s3 = boto3.client('s3')

    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json_string,
            ContentType='application/json'
        )
        print(f"✅ Uploaded {len(results.get('results', []))} jobs: {job_title} in {city}, {state}")
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


# --- Search List Generator ---
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


def generate_search_list(job_titles=None, states_cities=None):
    if states_cities is None:
        states_cities = states
    if job_titles is None:
        job_titles = job_titles
    return [
        {"Job_Title": jt, "State": state, "City": city}
        for jt in job_titles
        for state, cities in states_cities.items()
        for city in cities
    ]


# --- Main loop ---
def start_searching_for_adzuna_jobs():
    search_list = generate_search_list(job_titles=job_titles,states_cities=states)

    for i, search in enumerate(search_list, 1):
        job_title = search["Job_Title"]
        state = search["State"]
        city = search["City"]
        print(f"\n🔍 ({i}/{len(search_list)}) Searching: {job_title} in {city}, {state}")

        extract_from_adzuna(job_title, state, city)

        if i < len(search_list):  # Don't sleep after the last one
            print("⏳ Waiting 30 seconds to avoid rate limits...\n")
            time.sleep(30)


start_searching_for_adzuna_jobs()
