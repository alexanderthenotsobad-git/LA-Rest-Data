# /var/www/LA-Rest-Data/src/data_cleaner.py
#!/usr/bin/env python3
"""
Data cleaning module for restaurant data
Handles deduplication, validation, and formatting for Power BI
"""

import pandas as pd
import logging
import os
import re
from datetime import datetime


class DataCleaner:
    def __init__(self):
        """Initialize data cleaner with logging"""
        self.logger = logging.getLogger(__name__)
        self.raw_data_path = "data/raw/"
        self.processed_data_path = "data/processed/"
        
        # Ensure processed directory exists
        os.makedirs(self.processed_data_path, exist_ok=True)
    
    def load_raw_data(self, filename="la_restaurants_raw.csv"):
        """Load raw restaurant data from CSV"""
        try:
            filepath = os.path.join(self.processed_data_path, filename)
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                self.logger.info(f"Loaded {len(df)} records from {filename}")
                return df
            else:
                self.logger.error(f"File not found: {filepath}")
                return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            return pd.DataFrame()
    
    def clean_restaurant_names(self, df):
        """Clean and standardize restaurant names"""
        if 'Restaurant_Name' in df.columns:
            # Remove leading/trailing whitespace
            df['Restaurant_Name'] = df['Restaurant_Name'].str.strip()
            
            # Remove restaurants with generic or empty names
            invalid_names = ['Unknown', 'Restaurant', '', None]
            df = df[~df['Restaurant_Name'].isin(invalid_names)]
            df = df[df['Restaurant_Name'].notna()]
            
            self.logger.info(f"Cleaned restaurant names, {len(df)} records remaining")
        
        return df
    
    def clean_ratings_and_reviews(self, df):
        """Clean rating and review count data"""
        if 'Rating' in df.columns:
            # Convert to numeric and handle invalid values
            df['Rating'] = pd.to_numeric(df['Rating'], errors='coerce')
            
            # Filter reasonable rating range (0-5)
            df = df[(df['Rating'] >= 0) & (df['Rating'] <= 5)]
            
            # Round to 1 decimal place
            df['Rating'] = df['Rating'].round(1)
        
        if 'Review_Count' in df.columns:
            # Convert to integer and handle negatives
            df['Review_Count'] = pd.to_numeric(df['Review_Count'], errors='coerce').fillna(0)
            df['Review_Count'] = df['Review_Count'].astype(int)
            df = df[df['Review_Count'] >= 0]
        
        return df
    
    def clean_price_levels(self, df):
        """Standardize price level data"""
        if 'Price_Level' in df.columns:
            # Convert to numeric (0-4 scale)
            df['Price_Level'] = pd.to_numeric(df['Price_Level'], errors='coerce')
            
            # Filter valid price levels
            df.loc[~df['Price_Level'].isin([0, 1, 2, 3, 4]), 'Price_Level'] = None
        
        return df
    
    def clean_coordinates(self, df):
        """Validate and clean latitude/longitude data"""
        if 'Latitude' in df.columns and 'Longitude' in df.columns:
            # Convert to numeric
            df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
            df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
            
            # Filter LA area coordinates (rough bounds)
            la_bounds = {
                'lat_min': 33.7, 'lat_max': 34.3,
                'lon_min': -118.7, 'lon_max': -118.1
            }
            
            coord_filter = (
                (df['Latitude'] >= la_bounds['lat_min']) & 
                (df['Latitude'] <= la_bounds['lat_max']) &
                (df['Longitude'] >= la_bounds['lon_min']) & 
                (df['Longitude'] <= la_bounds['lon_max'])
            )
            
            df = df[coord_filter]
            self.logger.info(f"Filtered coordinates to LA area, {len(df)} records remaining")
        
        return df
    
    def clean_zip_codes(self, df):
        """Standardize ZIP code format"""
        if 'ZIP_Code' in df.columns:
            # Convert to string and extract 5-digit ZIP
            df['ZIP_Code'] = df['ZIP_Code'].astype(str)
            df['ZIP_Code'] = df['ZIP_Code'].apply(self._extract_zip)
            
            # Remove invalid ZIP codes
            df = df[df['ZIP_Code'].notna()]
            df = df[df['ZIP_Code'] != '']
        
        return df
    
    def _extract_zip(self, zip_str):
        """Extract 5-digit ZIP code from string"""
        if pd.isna(zip_str) or zip_str == 'nan':
            return None
        
        # Look for 5-digit pattern
        zip_match = re.search(r'\b(\d{5})\b', str(zip_str))
        return zip_match.group(1) if zip_match else None
    
    def remove_duplicates(self, df):
        """Remove duplicate restaurants"""
        initial_count = len(df)
        
        # Remove exact duplicates
        df = df.drop_duplicates()
        
        # Remove duplicates based on name and address
        if 'Restaurant_Name' in df.columns and 'Address' in df.columns:
            df = df.drop_duplicates(subset=['Restaurant_Name', 'Address'], keep='first')
        elif 'Restaurant_Name' in df.columns:
            df = df.drop_duplicates(subset=['Restaurant_Name'], keep='first')
        
        duplicates_removed = initial_count - len(df)
        if duplicates_removed > 0:
            self.logger.info(f"Removed {duplicates_removed} duplicate records")
        
        return df
    
    def add_derived_fields(self, df):
        """Add calculated fields for analysis"""
        # Price level labels
        if 'Price_Level' in df.columns:
            price_labels = {0: 'Free', 1: '$', 2: '$$', 3: '$$$', 4: '$$$$'}
            df['Price_Label'] = df['Price_Level'].map(price_labels)
        
        # Rating categories
        if 'Rating' in df.columns:
            df['Rating_Category'] = pd.cut(
                df['Rating'], 
                bins=[0, 3.0, 4.0, 5.0], 
                labels=['Below Average', 'Good', 'Excellent'],
                include_lowest=True
            )
        
        # Review volume categories
        if 'Review_Count' in df.columns:
            df['Review_Volume'] = pd.cut(
                df['Review_Count'],
                bins=[0, 10, 50, 200, float('inf')],
                labels=['Few Reviews', 'Some Reviews', 'Many Reviews', 'Very Popular'],
                include_lowest=True
            )
        
        return df
    
    def process_all_data(self, input_file="la_restaurants_final.csv"):
        """Main processing pipeline"""
        self.logger.info("Starting data cleaning pipeline...")
        
        # Load the data created by the extractor
        input_path = os.path.join(self.processed_data_path, input_file)
        if not os.path.exists(input_path):
            self.logger.error(f"Input file not found: {input_path}")
            return None
        
        df = pd.read_csv(input_path)
        initial_count = len(df)
        self.logger.info(f"Starting with {initial_count} records")
        
        # Apply cleaning steps
        df = self.clean_restaurant_names(df)
        df = self.clean_ratings_and_reviews(df)
        df = self.clean_price_levels(df)
        df = self.clean_coordinates(df)
        df = self.clean_zip_codes(df)
        df = self.remove_duplicates(df)
        df = self.add_derived_fields(df)
        
        # Save cleaned data
        output_file = "la_restaurants_cleaned.csv"
        output_path = os.path.join(self.processed_data_path, output_file)
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        final_count = len(df)
        self.logger.info(f"Cleaning completed: {final_count}/{initial_count} records retained")
        self.logger.info(f"Cleaned data saved to: {output_path}")
        
        # Print summary
        self._print_cleaning_summary(df, initial_count, final_count)
        
        return df
    
    def _print_cleaning_summary(self, df, initial_count, final_count):
        """Print data cleaning summary statistics"""
        print("\n" + "="*50)
        print("DATA CLEANING SUMMARY")
        print("="*50)
        print(f"Initial records: {initial_count}")
        print(f"Final records: {final_count}")
        print(f"Records removed: {initial_count - final_count}")
        print(f"Retention rate: {(final_count/initial_count)*100:.1f}%")
        
        if len(df) > 0:
            print(f"\nData Quality:")
            print(f"  Unique restaurants: {len(df)}")
            print(f"  Average rating: {df['Rating'].mean():.2f}")
            print(f"  ZIP codes covered: {df['ZIP_Code'].nunique()}")
            print(f"  Neighborhoods: {df['Neighborhood'].nunique()}")
            
            print(f"\nMissing Data:")
            for col in ['Rating', 'Price_Level', 'Review_Count']:
                if col in df.columns:
                    missing_pct = (df[col].isna().sum() / len(df)) * 100
                    print(f"  {col}: {missing_pct:.1f}% missing")
        
        print("="*50)
        print("✅ Data cleaning completed successfully!")
        print(f"📁 Output: data/processed/la_restaurants_cleaned.csv")