import requests

# Replace with your Adzuna credentials
APP_ID = '223d9f60'
APP_KEY = '3f4c7510782d76d9edffe71e90119545	'

# Set up base URL and endpoint
country = 'gb'  # Country code, e.g., 'gb' (UK), 'us', 'de', 'il'
endpoint = f'https://api.adzuna.com/v1/api/jobs/{country}/search/1'

# Define query parameters
params = {
    'app_id': APP_ID,
    'app_key': APP_KEY,
    'what': 'data analyst',       # Job title or keywords
    'where': 'London',            # Location
    'results_per_page': 5,
    'content-type': 'application/json'
}

# Make GET request
response = requests.get(endpoint, params=params)

# Check response
if response.status_code == 200:
    data = response.json()
    for i, job in enumerate(data.get('results', []), start=1):
        print(f"{i}. {job['title']} at {job['company']['display_name']}")
        print(f"   Location: {job['location']['display_name']}")
        print(f"   URL: {job['redirect_url']}\n")
else:
    print(f"Request failed with status {response.status_code}")
    print(response.text)
