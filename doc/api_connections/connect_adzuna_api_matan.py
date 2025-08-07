import requests

# Your Adzuna credentials
APP_ID = '223d9f60'
APP_KEY = '3f4c7510782d76d9edffe71e90119545'

# US endpoint
country = 'us'
endpoint = f'https://api.adzuna.com/v1/api/jobs/{country}/search/1'

# Search parameters
params = {
    'app_id': APP_ID,
    'app_key': APP_KEY,
    'what': 'Data Analyst',               # Job title
    'where': 'San Antonio, Texas',   # Use 'where' instead of location0/location1/location2
    'results_per_page': 20,
    'sort_by': 'date',                    # Optional: 'relevance' or 'date'
    'content-type': 'application/json'
}

# Make the GET request
response = requests.get(endpoint, params=params)

# Print request URL for debugging
print("Request URL:", response.url)

# Process the response
if response.status_code == 200:
    data = response.json()
    results = data.get('results', [])
    if not results:
        print("✅ Request successful but no job results found.")
    else:
        print(f"✅ Found {len(results)} job ads:\n")
        for i, job in enumerate(results, 1):
            print(f"{i}. {job['title']} at {job['company']['display_name']}")
            print(f"   Location: {job['location']['display_name']}")
            print(f"   URL: {job['redirect_url']}\n")
else:
    print(f"❌ Request failed with status {response.status_code}")
    print("Response:", response.text)
