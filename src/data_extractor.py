import requests
import json
import pandas as pd
from datetime import datetime
from config.config import API_KEY, BASE_URL, TARGET_AREAS

class RestaurantExtractor:
    def __init__(self):
        self.api_key = API_KEY
        self.base_url = BASE_URL
        self.call_count = 0
    
    def extract_area_restaurants(self, area_config):
        # API extraction logic here
        pass
    
    def save_raw_data(self, data, area_name):
        # Save JSON to data/raw/
        pass