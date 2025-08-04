import requests

# Replace with your email and API key from USAJOBS
USER_AGENT = 'avitalho87@gmail.com'
API_KEY = 'Q2wSsBtvsNOFJ4vW102mx9PMLaYwbKz8WoGjEkLcGto='

# Set up base URL
endpoint = 'https://data.usajobs.gov/api/search'

# Define query parameters
params = {
    'Keyword': 'Data Analyst',
    'LocationName': 'Washington, DC',
    'ResultsPerPage': 5
}

# Define required headers
headers = {
    'Host': 'data.usajobs.gov',
    'User-Agent': USER_AGENT,
    'Authorization-Key': API_KEY
}

# Make GET request
response = requests.get(endpoint, params=params, headers=headers)

# Handle response
if response.status_code == 200:
    data = response.json()
    for i, job in enumerate(data.get('SearchResult', {}).get('SearchResultItems', []), start=1):
        job_data = job['MatchedObjectDescriptor']
        print(f"{i}. {job_data['PositionTitle']} at {job_data['OrganizationName']}")
        print(f"   Location: {job_data['PositionLocation'][0]['LocationName']}")
        print(f"   URL: {job_data['PositionURI']}\n")
else:
    print(f"Request failed with status {response.status_code}")
    print(response.text)
