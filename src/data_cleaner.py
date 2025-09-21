# /var/www/LA-Rest-Data/src/data_cleaner.py
#!/usr/bin/env python3
"""
Enhanced data cleaning and processing for LA County restaurant data
Adds calculated fields for Power BI analysis
"""

import pandas as pd
import numpy as np
import logging
import os

class DataCleaner:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def load_data(self, filepath='data/processed/la_county_restaurants_final.csv'):
        """Load the restaurant data"""
        try:
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                self.logger.info(f"Loaded {len(df)} records from {filepath}")
                return df
            else:
                self.logger.warning(f"File not found: {filepath}")
                return None
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            return None
    
    def add_calculated_fields(self, df):
        """Add calculated fields for Power BI analysis"""
        
        # Price Label Column
        def get_price_label(price_level):
            if pd.isna(price_level):
                return "Unknown"
            price_map = {0: "Free", 1: "$", 2: "$$", 3: "$$$", 4: "$$$$"}
            return price_map.get(int(price_level), "Unknown")
        
        df['Price_Label'] = df['Price_Level'].apply(get_price_label)
        
        # Rating Category Column
        def get_rating_category(rating):
            if pd.isna(rating):
                return "No Rating"
            elif rating >= 4.5:
                return "Excellent (4.5+)"
            elif rating >= 4.0:
                return "Very Good (4.0-4.4)"
            elif rating >= 3.5:
                return "Good (3.5-3.9)"
            elif rating >= 3.0:
                return "Average (3.0-3.4)"
            else:
                return "Below Average (<3.0)"
        
        df['Rating_Category'] = df['Rating'].apply(get_rating_category)
        
        # Review Volume Category
        def get_review_volume(review_count):
            if pd.isna(review_count) or review_count == 0:
                return "No Reviews"
            elif review_count >= 1000:
                return "High Volume (1000+)"
            elif review_count >= 500:
                return "Medium Volume (500-999)"
            elif review_count >= 100:
                return "Low Volume (100-499)"
            else:
                return "Very Low Volume (<100)"
        
        df['Review_Volume'] = df['Review_Count'].apply(get_review_volume)
        
        # Restaurant density by ZIP code
        zip_counts = df['ZIP_Code'].value_counts().to_dict()
        df['Restaurants_in_ZIP'] = df['ZIP_Code'].map(zip_counts)
        
        # Average rating by ZIP code
        zip_avg_ratings = df.groupby('ZIP_Code')['Rating'].mean().to_dict()
        df['ZIP_Avg_Rating'] = df['ZIP_Code'].map(zip_avg_ratings)
        
        # Restaurant density by neighborhood
        neighborhood_counts = df['Neighborhood'].value_counts().to_dict()
        df['Restaurants_in_Neighborhood'] = df['Neighborhood'].map(neighborhood_counts)
        
        # Average rating by neighborhood
        neighborhood_avg_ratings = df.groupby('Neighborhood')['Rating'].mean().to_dict()
        df['Neighborhood_Avg_Rating'] = df['Neighborhood'].map(neighborhood_avg_ratings)
        
        # Market saturation index (combines density + average rating)
        def calculate_market_saturation(row):
            if pd.isna(row['ZIP_Avg_Rating']) or pd.isna(row['Restaurants_in_ZIP']):
                return None
            
            # Normalize values
            normalized_count = min(row['Restaurants_in_ZIP'] / 20, 1)  # Cap at 20 restaurants = 1.0
            normalized_rating = row['ZIP_Avg_Rating'] / 5.0  # Rating scale 0-5
            
            # Weighted combination (60% density, 40% rating)
            return round((normalized_count * 0.6) + (normalized_rating * 0.4), 3)
        
        df['Market_Saturation_Index'] = df.apply(calculate_market_saturation, axis=1)
        
        # High-value target indicator (high rating, low competition)
        def is_high_value_target(row):
            if pd.isna(row['ZIP_Avg_Rating']) or pd.isna(row['Restaurants_in_ZIP']):
                return "Unknown"
            
            avg_rating = row['ZIP_Avg_Rating']
            restaurant_count = row['Restaurants_in_ZIP']
            
            if avg_rating >= 4.0 and restaurant_count <= 10:
                return "High Value Target"
            elif avg_rating >= 3.5 and restaurant_count <= 15:
                return "Moderate Value Target"
            elif restaurant_count >= 30:
                return "Saturated Market"
            else:
                return "Standard Market"
        
        df['Market_Opportunity'] = df.apply(is_high_value_target, axis=1)
        
        return df
    
    def clean_data_quality(self, df):
        """Clean data quality issues"""
        
        # Remove obvious data quality issues
        initial_count = len(df)
        
        # Remove restaurants with invalid coordinates (0,0 or extreme outliers)
        df = df[~((df['Latitude'] == 0) & (df['Longitude'] == 0))]
        
        # Remove restaurants outside reasonable LA County bounds
        # LA County approximate bounds: 33.7°N to 34.8°N, -118.9°W to -117.6°W
        df = df[
            (df['Latitude'] >= 33.5) & (df['Latitude'] <= 35.0) &
            (df['Longitude'] >= -119.5) & (df['Longitude'] <= -117.0)
        ]
        
        # Remove duplicate restaurants (same name + similar location)
        df = df.drop_duplicates(subset=['Restaurant_Name', 'ZIP_Code'], keep='first')
        
        # Clean restaurant names
        df['Restaurant_Name'] = df['Restaurant_Name'].str.strip()
        df['Restaurant_Name'] = df['Restaurant_Name'].str.title()
        
        # Clean neighborhood names
        df['Neighborhood'] = df['Neighborhood'].str.strip()
        df['Neighborhood'] = df['Neighborhood'].str.title()
        
        # Standardize ZIP codes (ensure 5 digits)
        df['ZIP_Code'] = df['ZIP_Code'].astype(str).str.zfill(5)
        
        final_count = len(df)
        removed = initial_count - final_count
        
        if removed > 0:
            self.logger.info(f"Data cleaning removed {removed} records")
            
        return df
    
    def create_summary_stats(self, df):
        """Create summary statistics for reporting"""
        
        stats = {
            'total_restaurants': len(df),
            'unique_zip_codes': df['ZIP_Code'].nunique(),
            'unique_neighborhoods': df['Neighborhood'].nunique(),
            'avg_rating': df['Rating'].mean(),
            'median_rating': df['Rating'].median(),
            'avg_review_count': df['Review_Count'].mean(),
            'price_distribution': df['Price_Label'].value_counts().to_dict(),
            'rating_distribution': df['Rating_Category'].value_counts().to_dict(),
            'top_zip_codes': df['ZIP_Code'].value_counts().head(10).to_dict(),
            'top_neighborhoods': df['Neighborhood'].value_counts().head(10).to_dict()
        }
        
        return stats
    
    def save_enhanced_data(self, df, filename='data/processed/la_restaurants_cleaned.csv'):
        """Save the enhanced dataset"""
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            df.to_csv(filename, index=False, encoding='utf-8')
            self.logger.info(f"Enhanced data saved to {filename}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving enhanced data: {str(e)}")
            return False
    
    def process_all_data(self):
        """Main processing method"""
        
        self.logger.info("Starting enhanced data processing...")
        
        # Load data
        df = self.load_data()
        if df is None:
            self.logger.error("Failed to load data. Cannot proceed.")
            return
        
        # Clean data quality
        df = self.clean_data_quality(df)
        
        # Add calculated fields
        df = self.add_calculated_fields(df)
        
        # Create summary statistics
        stats = self.create_summary_stats(df)
        
        # Save enhanced data
        success = self.save_enhanced_data(df)
        
        if success:
            print("\n" + "="*60)
            print("ENHANCED DATA PROCESSING COMPLETE")
            print("="*60)
            print(f"📊 Total restaurants processed: {stats['total_restaurants']}")
            print(f"📍 ZIP codes covered: {stats['unique_zip_codes']}")
            print(f"🏘️  Neighborhoods covered: {stats['unique_neighborhoods']}")
            print(f"⭐ Average rating: {stats['avg_rating']:.2f}")
            print(f"📝 Average reviews per restaurant: {stats['avg_review_count']:.0f}")
            
            print("\n💰 Price Distribution:")
            for price, count in sorted(stats['price_distribution'].items()):
                print(f"   {price}: {count} restaurants")
            
            print("\n⭐ Rating Distribution:")
            for rating, count in sorted(stats['rating_distribution'].items()):
                print(f"   {rating}: {count} restaurants")
            
            print("\n📁 Files created:")
            print("   • la_restaurants_cleaned.csv (enhanced dataset with calculated fields)")
            print("   • Ready for Power BI import with pre-calculated analysis fields!")
            
            print("\n🔍 New calculated fields added:")
            print("   • Price_Label (readable price categories)")
            print("   • Rating_Category (rating groupings)")
            print("   • Review_Volume (review count categories)")
            print("   • Market_Saturation_Index (density + rating combined)")
            print("   • Market_Opportunity (business opportunity analysis)")
            print("   • ZIP/Neighborhood statistics")
            
        else:
            print("❌ Failed to save enhanced data")
        
        self.logger.info("Data cleaning and processing completed")

if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.process_all_data()