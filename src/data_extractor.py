# /var/www/LA-Rest-Data/src/data_extractor.py
#!/usr/bin/env python3
"""
Google Places API Restaurant Data Extractor for LA Power BI Dashboard
Refactored from original Yelp API approach
"""

import requests
import pandas as pd
import json
import time
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config/.env')

class GooglePlacesRestaurantExtractor:
    def __init__(self):
        self.api_key = os.getenv('GOOGLE_PLACES_API_KEY')
        self.base_url = "https://places.googleapis.com/v1/places:searchText"
        self.call_count = 0
        self.daily_limit = 5000
        self.restaurants_data = []
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/extraction.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        if not self.api_key:
            raise ValueError("API key not found. Please check your config/.env file")
    
    def get_headers(self):
        """Generate headers for Google Places API (New)"""
        return {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.api_key,
            'X-Goog-FieldMask': (
                'places.displayName,'
                'places.rating,'
                'places.userRatingCount,'
                'places.priceLevel,'
                'places.location,'
                'places.primaryTypeDisplayName,'
                'places.formattedAddress,'
                'places.types'
            )
        }
    
    def search_restaurants_by_area(self, search_query, max_results=20):
        """
        Search for restaurants in a specific area using Google Places API (New)
        """
        if self.call_count >= self.daily_limit:
            self.logger.warning(f"Daily API limit ({self.daily_limit}) reached!")
            return []
        
        headers = self.get_headers()
        data = {
            "textQuery": search_query,
            "maxResultCount": max_results,
            "includedType": "restaurant"
        }
        
        try:
            self.logger.info(f"Searching: {search_query}")
            response = requests.post(self.base_url, headers=headers, json=data)
            self.call_count += 1
            
            # Rate limiting - Google recommends not exceeding 1000 QPS
            time.sleep(0.1)
            
            if response.status_code == 200:
                results = response.json()
                restaurants = results.get('places', [])
                self.logger.info(f"Found {len(restaurants)} restaurants")
                return restaurants
            else:
                self.logger.error(f"API Error {response.status_code}: {response.text}")
                return []
                
        except Exception as e:
            self.logger.error(f"Exception during API call: {str(e)}")
            return []
    
    def extract_restaurant_data(self, place, neighborhood="Unknown"):
        """
        Extract relevant data from a single restaurant place object
        """
        try:
            # Basic information
            name = place.get('displayName', {}).get('text', 'Unknown')
            rating = place.get('rating', 0.0)
            review_count = place.get('userRatingCount', 0)
            
            # Price level (Google uses PRICE_LEVEL_INEXPENSIVE, etc.)
            price_level_map = {
                'PRICE_LEVEL_FREE': 0,
                'PRICE_LEVEL_INEXPENSIVE': 1,
                'PRICE_LEVEL_MODERATE': 2,
                'PRICE_LEVEL_EXPENSIVE': 3,
                'PRICE_LEVEL_VERY_EXPENSIVE': 4
            }
            price_level = price_level_map.get(place.get('priceLevel'), None)
            
            # Location data
            location = place.get('location', {})
            latitude = location.get('latitude', 0.0)
            longitude = location.get('longitude', 0.0)
            
            # Address and ZIP code extraction
            address = place.get('formattedAddress', '')
            zip_code = self.extract_zip_code(address)
            
            # Category/cuisine type
            category = place.get('primaryTypeDisplayName', {}).get('text', 'Restaurant')
            types = place.get('types', [])
            if 'restaurant' in [t.lower() for t in types]:
                # Look for more specific cuisine types
                cuisine_types = [t for t in types if 'restaurant' not in t.lower()]
                if cuisine_types:
                    category = cuisine_types[0].replace('_', ' ').title()
            
            return {
                'ZIP_Code': zip_code,
                'Restaurant_Name': name,
                'Rating': round(rating, 1) if rating else None,
                'Review_Count': review_count,
                'Price_Level': price_level,
                'Latitude': round(latitude, 6) if latitude else None,
                'Longitude': round(longitude, 6) if longitude else None,
                'Category': category,
                'Neighborhood': neighborhood,
                'Address': address
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting data for place: {str(e)}")
            return None
    
    def extract_zip_code(self, address):
        """
        Extract ZIP code from formatted address
        """
        import re
        if not address:
            return None
        
        # Look for 5-digit ZIP code pattern
        zip_match = re.search(r'\b\d{5}(?:-\d{4})?\b', address)
        if zip_match:
            return zip_match.group()[:5]  # Return just 5-digit ZIP
        return None
    
    def get_la_search_areas(self):
        """
        Define LA neighborhoods and areas to search
        """
        return [
            {"name": "Beverly Hills", "query": "restaurants in Beverly Hills, CA", "zip_hint": "90210"},
            {"name": "Santa Monica", "query": "restaurants in Santa Monica, CA", "zip_hint": "90401"},
            {"name": "Hollywood", "query": "restaurants in Hollywood, CA", "zip_hint": "90028"},
            {"name": "West Hollywood", "query": "restaurants in West Hollywood, CA", "zip_hint": "90069"},
            {"name": "Venice", "query": "restaurants in Venice, CA", "zip_hint": "90291"},
            {"name": "Downtown LA", "query": "restaurants in Downtown Los Angeles, CA", "zip_hint": "90015"},
            {"name": "Pasadena", "query": "restaurants in Pasadena, CA", "zip_hint": "91101"},
            {"name": "Culver City", "query": "restaurants in Culver City, CA", "zip_hint": "90232"},
            {"name": "Manhattan Beach", "query": "restaurants in Manhattan Beach, CA", "zip_hint": "90266"},
            {"name": "Redondo Beach", "query": "restaurants in Redondo Beach, CA", "zip_hint": "90277"},
            {"name": "Long Beach", "query": "restaurants in Long Beach, CA", "zip_hint": "90802"},
            {"name": "Burbank", "query": "restaurants in Burbank, CA", "zip_hint": "91502"},
            {"name": "Glendale", "query": "restaurants in Glendale, CA", "zip_hint": "91201"},
            {"name": "Inglewood", "query": "restaurants in Inglewood, CA", "zip_hint": "90301"},
            {"name": "El Segundo", "query": "restaurants in El Segundo, CA", "zip_hint": "90245"}
        ]
    
    def run_full_extraction(self):
        """
        Main extraction method - searches all LA areas
        """
        self.logger.info("Starting LA Restaurant Data Extraction...")
        start_time = datetime.now()
        
        search_areas = self.get_la_search_areas()
        total_restaurants = 0
        
        for area in search_areas:
            area_name = area["name"]
            search_query = area["query"]
            
            # Search for restaurants in this area
            places = self.search_restaurants_by_area(search_query, max_results=20)
            
            # Extract data for each restaurant
            for place in places:
                restaurant_data = self.extract_restaurant_data(place, area_name)
                if restaurant_data:
                    # If ZIP code extraction failed, use hint
                    if not restaurant_data['ZIP_Code']:
                        restaurant_data['ZIP_Code'] = area["zip_hint"]
                    
                    self.restaurants_data.append(restaurant_data)
                    total_restaurants += 1
            
            self.logger.info(f"Extracted {len(places)} restaurants from {area_name}")
            
            # Save raw data backup
            self.save_raw_data(places, area_name)
            
            # Respect API limits - small delay between areas
            time.sleep(0.5)
        
        end_time = datetime.now()
        duration = (end_time - start_time).seconds
        
        self.logger.info(f"Extraction complete! Total restaurants: {total_restaurants}")
        self.logger.info(f"API calls used: {self.call_count}")
        self.logger.info(f"Duration: {duration} seconds")
        
        return self.restaurants_data
    
    def save_raw_data(self, data, area_name):
        """
        Save raw JSON responses for backup
        """
        try:
            filename = f"data/raw/{area_name.lower().replace(' ', '_')}.json"
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            self.logger.error(f"Error saving raw data for {area_name}: {str(e)}")
    
    def save_to_csv(self, filename='data/processed/la_restaurants_final.csv'):
        """
        Save extracted restaurant data to CSV for Power BI
        """
        if not self.restaurants_data:
            self.logger.warning("No restaurant data to save!")
            return
        
        try:
            # Create DataFrame
            df = pd.DataFrame(self.restaurants_data)
            
            # Remove duplicates based on name and address
            initial_count = len(df)
            df = df.drop_duplicates(subset=['Restaurant_Name', 'Address'], keep='first')
            final_count = len(df)
            
            if initial_count != final_count:
                self.logger.info(f"Removed {initial_count - final_count} duplicates")
            
            # Clean and validate data
            df = df[df['Restaurant_Name'] != 'Unknown']  # Remove unknown names
            df = df[df['Latitude'].notna() & df['Longitude'].notna()]  # Remove missing coordinates
            
            # Create processed directory
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            # Save to CSV
            df.to_csv(filename, index=False, encoding='utf-8')
            
            self.logger.info(f"Saved {len(df)} restaurants to {filename}")
            
            # Print summary statistics
            self.print_summary_stats(df)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error saving CSV: {str(e)}")
            return None
    
    def print_summary_stats(self, df):
        """
        Print summary statistics about the extracted data
        """
        print("\n" + "="*50)
        print("EXTRACTION SUMMARY")
        print("="*50)
        print(f"Total Restaurants: {len(df)}")
        print(f"Unique ZIP Codes: {df['ZIP_Code'].nunique()}")
        print(f"Average Rating: {df['Rating'].mean():.2f}")
        print(f"Neighborhoods Covered: {df['Neighborhood'].nunique()}")
        
        print("\nPrice Level Distribution:")
        price_counts = df['Price_Level'].value_counts().sort_index()
        for price, count in price_counts.items():
            price_label = ["Free", "$", "$$", "$$$", "$$$$"][int(price)] if price is not None else "Unknown"
            print(f"  {price_label}: {count}")
        
        print("\nTop 10 ZIP Codes by Restaurant Count:")
        zip_counts = df['ZIP_Code'].value_counts().head(10)
        for zip_code, count in zip_counts.items():
            print(f"  {zip_code}: {count}")
        
        print("="*50)

def main():
    """
    Main execution function
    """
    try:
        # Initialize extractor
        extractor = GooglePlacesRestaurantExtractor()
        
        # Run extraction
        restaurants = extractor.run_full_extraction()
        
        # Save results
        df = extractor.save_to_csv()
        
        if df is not None:
            print(f"\n✅ SUCCESS! Extracted {len(df)} restaurants")
            print("📁 Data saved to: data/processed/la_restaurants_final.csv")
            print("🔄 Ready for Power BI import!")
        else:
            print("❌ ERROR: Failed to save data")
            
    except Exception as e:
        print(f"❌ FATAL ERROR: {str(e)}")
        logging.error(f"Fatal error in main: {str(e)}")

if __name__ == "__main__":
    main()