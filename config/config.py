import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Configuration
API_KEY = os.getenv('AIzaSyAtzg-ijVkubyZzFww6iNusPi3Zb_GtvXg')
BASE_URL = "https://places.googleapis.com/v1/places:searchText"
MAX_RESULTS_PER_CALL = 20
DAILY_CALL_LIMIT = 5000

# LA Neighborhoods to Target
TARGET_AREAS = [
    {"name": "Beverly Hills", "query": "restaurants in Beverly Hills, CA"},
    {"name": "Santa Monica", "query": "restaurants in Santa Monica, CA"},
    {"name": "Hollywood", "query": "restaurants in Hollywood, CA"},
    # ... more areas
]