import requests

# Replace with your Adzuna credentials
APP_ID = '85001579'
APP_KEY = '3f42741594f1135c2c9a6e2f601b509a'

# Set up base URL and endpoint
country = 'gb'  # Country code, e.g., 'gb' (UK), 'us', 'de', 'il'
endpoint = f'https://api.adzuna.com/v1/api/jobs/{country}/search/1'

# Define query parameters
params = {
    'app_id': APP_ID,
    'app_key': APP_KEY,
    'what': 'Data Analyst',       # Job title or keywords
    'location0': 'UK',
    'location1': 'London',
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
