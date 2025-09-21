# /var/www/LA-Rest-Data/src/data_extractor.py
#!/usr/bin/env python3
"""
Google Places API Restaurant Data Extractor for LA County Power BI Dashboard
Expanded to cover all major areas of Los Angeles County
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

class LACountyRestaurantExtractor:
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
    
    def get_la_county_search_areas(self):
        """
        Define LA County cities and areas to search - Expanded Coverage
        """
        return [
            # Original LA City Areas
            {"name": "Beverly Hills", "query": "restaurants in Beverly Hills, CA", "zip_hint": "90210"},
            {"name": "Santa Monica", "query": "restaurants in Santa Monica, CA", "zip_hint": "90401"},
            {"name": "Hollywood", "query": "restaurants in Hollywood, CA", "zip_hint": "90028"},
            {"name": "West Hollywood", "query": "restaurants in West Hollywood, CA", "zip_hint": "90069"},
            {"name": "Venice", "query": "restaurants in Venice, CA", "zip_hint": "90291"},
            {"name": "Downtown LA", "query": "restaurants in Downtown Los Angeles, CA", "zip_hint": "90015"},
            
            # San Fernando Valley
            {"name": "North Hollywood", "query": "restaurants in North Hollywood, CA", "zip_hint": "91601"},
            {"name": "Van Nuys", "query": "restaurants in Van Nuys, CA", "zip_hint": "91401"},
            {"name": "Sherman Oaks", "query": "restaurants in Sherman Oaks, CA", "zip_hint": "91403"},
            {"name": "Studio City", "query": "restaurants in Studio City, CA", "zip_hint": "91604"},
            {"name": "Encino", "query": "restaurants in Encino, CA", "zip_hint": "91316"},
            {"name": "Tarzana", "query": "restaurants in Tarzana, CA", "zip_hint": "91356"},
            {"name": "Woodland Hills", "query": "restaurants in Woodland Hills, CA", "zip_hint": "91364"},
            {"name": "Canoga Park", "query": "restaurants in Canoga Park, CA", "zip_hint": "91303"},
            {"name": "Reseda", "query": "restaurants in Reseda, CA", "zip_hint": "91335"},
            {"name": "Northridge", "query": "restaurants in Northridge, CA", "zip_hint": "91324"},
            {"name": "Granada Hills", "query": "restaurants in Granada Hills, CA", "zip_hint": "91344"},
            {"name": "Chatsworth", "query": "restaurants in Chatsworth, CA", "zip_hint": "91311"},
            {"name": "Porter Ranch", "query": "restaurants in Porter Ranch, CA", "zip_hint": "91326"},
            
            # Westside
            {"name": "Brentwood", "query": "restaurants in Brentwood, CA", "zip_hint": "90049"},
            {"name": "Westwood", "query": "restaurants in Westwood, CA", "zip_hint": "90024"},
            {"name": "Pacific Palisades", "query": "restaurants in Pacific Palisades, CA", "zip_hint": "90272"},
            {"name": "Malibu", "query": "restaurants in Malibu, CA", "zip_hint": "90265"},
            {"name": "Manhattan Beach", "query": "restaurants in Manhattan Beach, CA", "zip_hint": "90266"},
            {"name": "Redondo Beach", "query": "restaurants in Redondo Beach, CA", "zip_hint": "90277"},
            {"name": "Hermosa Beach", "query": "restaurants in Hermosa Beach, CA", "zip_hint": "90254"},
            
            # South Bay
            {"name": "Torrance", "query": "restaurants in Torrance, CA", "zip_hint": "90501"},
            {"name": "Carson", "query": "restaurants in Carson, CA", "zip_hint": "90745"},
            {"name": "Gardena", "query": "restaurants in Gardena, CA", "zip_hint": "90247"},
            {"name": "Hawthorne", "query": "restaurants in Hawthorne, CA", "zip_hint": "90250"},
            {"name": "Lawndale", "query": "restaurants in Lawndale, CA", "zip_hint": "90260"},
            {"name": "Lomita", "query": "restaurants in Lomita, CA", "zip_hint": "90717"},
            {"name": "Palos Verdes", "query": "restaurants in Palos Verdes, CA", "zip_hint": "90274"},
            
            # San Gabriel Valley
            {"name": "Pasadena", "query": "restaurants in Pasadena, CA", "zip_hint": "91101"},
            {"name": "Alhambra", "query": "restaurants in Alhambra, CA", "zip_hint": "91801"},
            {"name": "Arcadia", "query": "restaurants in Arcadia, CA", "zip_hint": "91006"},
            {"name": "Monrovia", "query": "restaurants in Monrovia, CA", "zip_hint": "91016"},
            {"name": "Azusa", "query": "restaurants in Azusa, CA", "zip_hint": "91702"},
            {"name": "Covina", "query": "restaurants in Covina, CA", "zip_hint": "91722"},
            {"name": "West Covina", "query": "restaurants in West Covina, CA", "zip_hint": "91790"},
            {"name": "Pomona", "query": "restaurants in Pomona, CA", "zip_hint": "91766"},
            {"name": "Claremont", "query": "restaurants in Claremont, CA", "zip_hint": "91711"},
            {"name": "La Verne", "query": "restaurants in La Verne, CA", "zip_hint": "91750"},
            {"name": "San Dimas", "query": "restaurants in San Dimas, CA", "zip_hint": "91773"},
            {"name": "Diamond Bar", "query": "restaurants in Diamond Bar, CA", "zip_hint": "91765"},
            {"name": "Walnut", "query": "restaurants in Walnut, CA", "zip_hint": "91789"},
            {"name": "Monterey Park", "query": "restaurants in Monterey Park, CA", "zip_hint": "91754"},
            {"name": "San Gabriel", "query": "restaurants in San Gabriel, CA", "zip_hint": "91776"},
            {"name": "Rosemead", "query": "restaurants in Rosemead, CA", "zip_hint": "91770"},
            {"name": "El Monte", "query": "restaurants in El Monte, CA", "zip_hint": "91731"},
            {"name": "Baldwin Park", "query": "restaurants in Baldwin Park, CA", "zip_hint": "91706"},
            {"name": "Temple City", "query": "restaurants in Temple City, CA", "zip_hint": "91780"},
            {"name": "San Marino", "query": "restaurants in San Marino, CA", "zip_hint": "91108"},
            
            # East LA / Southeast Cities
            {"name": "Burbank", "query": "restaurants in Burbank, CA", "zip_hint": "91502"},
            {"name": "Glendale", "query": "restaurants in Glendale, CA", "zip_hint": "91201"},
            {"name": "Long Beach", "query": "restaurants in Long Beach, CA", "zip_hint": "90802"},
            {"name": "Culver City", "query": "restaurants in Culver City, CA", "zip_hint": "90232"},
            {"name": "Inglewood", "query": "restaurants in Inglewood, CA", "zip_hint": "90301"},
            {"name": "El Segundo", "query": "restaurants in El Segundo, CA", "zip_hint": "90245"},
            {"name": "Montebello", "query": "restaurants in Montebello, CA", "zip_hint": "90640"},
            {"name": "Pico Rivera", "query": "restaurants in Pico Rivera, CA", "zip_hint": "90660"},
            {"name": "Downey", "query": "restaurants in Downey, CA", "zip_hint": "90241"},
            {"name": "Norwalk", "query": "restaurants in Norwalk, CA", "zip_hint": "90650"},
            {"name": "Whittier", "query": "restaurants in Whittier, CA", "zip_hint": "90601"},
            {"name": "Cerritos", "query": "restaurants in Cerritos, CA", "zip_hint": "90703"},
            {"name": "Artesia", "query": "restaurants in Artesia, CA", "zip_hint": "90701"},
            {"name": "Bellflower", "query": "restaurants in Bellflower, CA", "zip_hint": "90706"},
            {"name": "Lakewood", "query": "restaurants in Lakewood, CA", "zip_hint": "90712"},
            {"name": "Paramount", "query": "restaurants in Paramount, CA", "zip_hint": "90723"},
            {"name": "Lynwood", "query": "restaurants in Lynwood, CA", "zip_hint": "90262"},
            {"name": "South Gate", "query": "restaurants in South Gate, CA", "zip_hint": "90280"},
            {"name": "Huntington Park", "query": "restaurants in Huntington Park, CA", "zip_hint": "90255"},
            {"name": "Bell", "query": "restaurants in Bell, CA", "zip_hint": "90201"},
            {"name": "Bell Gardens", "query": "restaurants in Bell Gardens, CA", "zip_hint": "90201"},
            {"name": "Cudahy", "query": "restaurants in Cudahy, CA", "zip_hint": "90201"},
            {"name": "Maywood", "query": "restaurants in Maywood, CA", "zip_hint": "90270"},
            {"name": "Commerce", "query": "restaurants in Commerce, CA", "zip_hint": "90040"},
            {"name": "Vernon", "query": "restaurants in Vernon, CA", "zip_hint": "90058"},
            {"name": "Signal Hill", "query": "restaurants in Signal Hill, CA", "zip_hint": "90755"},
            
            # Antelope Valley
            {"name": "Lancaster", "query": "restaurants in Lancaster, CA", "zip_hint": "93534"},
            {"name": "Palmdale", "query": "restaurants in Palmdale, CA", "zip_hint": "93550"},
            
            # Santa Clarita Valley
            {"name": "Santa Clarita", "query": "restaurants in Santa Clarita, CA", "zip_hint": "91350"},
            {"name": "Valencia", "query": "restaurants in Valencia, CA", "zip_hint": "91355"},
            {"name": "Newhall", "query": "restaurants in Newhall, CA", "zip_hint": "91321"},
            {"name": "Canyon Country", "query": "restaurants in Canyon Country, CA", "zip_hint": "91387"},
            {"name": "Castaic", "query": "restaurants in Castaic, CA", "zip_hint": "91384"},
            
            # Foothill Communities
            {"name": "Altadena", "query": "restaurants in Altadena, CA", "zip_hint": "91001"},
            {"name": "La Canada", "query": "restaurants in La Canada Flintridge, CA", "zip_hint": "91011"},
            {"name": "Sierra Madre", "query": "restaurants in Sierra Madre, CA", "zip_hint": "91024"},
            {"name": "Duarte", "query": "restaurants in Duarte, CA", "zip_hint": "91010"},
            
            # West San Gabriel Valley
            {"name": "Calabasas", "query": "restaurants in Calabasas, CA", "zip_hint": "91302"},
            {"name": "Agoura Hills", "query": "restaurants in Agoura Hills, CA", "zip_hint": "91301"},
            {"name": "Hidden Hills", "query": "restaurants in Hidden Hills, CA", "zip_hint": "91302"},
        ]
    
    def run_full_extraction(self):
        """
        Main extraction method - searches all LA County areas
        """
        self.logger.info("Starting LA County Restaurant Data Extraction...")
        start_time = datetime.now()
        
        search_areas = self.get_la_county_search_areas()
        total_restaurants = 0
        
        self.logger.info(f"Will search {len(search_areas)} areas across LA County")
        
        for i, area in enumerate(search_areas, 1):
            area_name = area["name"]
            search_query = area["query"]
            
            self.logger.info(f"Processing area {i}/{len(search_areas)}: {area_name}")
            
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
        self.logger.info(f"Areas searched: {len(search_areas)}")
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
    
    def save_to_csv(self, filename='data/processed/la_county_restaurants_final.csv'):
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
        print("\n" + "="*60)
        print("LA COUNTY RESTAURANT EXTRACTION SUMMARY")
        print("="*60)
        print(f"Total Restaurants: {len(df)}")
        print(f"Unique ZIP Codes: {df['ZIP_Code'].nunique()}")
        print(f"Average Rating: {df['Rating'].mean():.2f}")
        print(f"Neighborhoods Covered: {df['Neighborhood'].nunique()}")
        
        print("\nPrice Level Distribution:")
        price_counts = df['Price_Level'].value_counts().sort_index()
        for price, count in price_counts.items():
            price_label = ["Free", "$", "$$", "$$$", "$$$$"][int(price)] if price is not None else "Unknown"
            print(f"  {price_label}: {count}")
        
        print("\nTop 15 ZIP Codes by Restaurant Count:")
        zip_counts = df['ZIP_Code'].value_counts().head(15)
        for zip_code, count in zip_counts.items():
            print(f"  {zip_code}: {count}")
        
        print("\nTop 15 Neighborhoods by Restaurant Count:")
        neighborhood_counts = df['Neighborhood'].value_counts().head(15)
        for neighborhood, count in neighborhood_counts.items():
            print(f"  {neighborhood}: {count}")
        
        print("="*60)

def main():
    """
    Main execution function
    """
    try:
        # Initialize extractor
        extractor = LACountyRestaurantExtractor()
        
        # Run extraction
        restaurants = extractor.run_full_extraction()
        
        # Save results
        df = extractor.save_to_csv()
        
        if df is not None:
            print(f"\n✅ SUCCESS! Extracted {len(df)} restaurants from LA County")
            print("📁 Data saved to: data/processed/la_county_restaurants_final.csv")
            print("📊 Ready for Power BI import!")
        else:
            print("❌ ERROR: Failed to save data")
            
    except Exception as e:
        print(f"❌ FATAL ERROR: {str(e)}")
        logging.error(f"Fatal error in main: {str(e)}")

if __name__ == "__main__":
    main()