job_titles = [
    "Data Analyst",
    "Software Engineer",
    "Marketing Manager",
    "UX Designer",
    "Financial Analyst",
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

scan_list = generate_search_list(job_titles=job_titles, states_cities=states)
for entry in scan_list:
    print(entry)