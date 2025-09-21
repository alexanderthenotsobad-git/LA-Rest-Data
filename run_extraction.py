# /var/www/LA-Rest-Data/run_extraction.py
#!/usr/bin/env python3
"""
Main script to run LA County restaurant data extraction
Usage: python run_extraction.py
"""

from src.data_extractor import LACountyRestaurantExtractor
from src.data_cleaner import DataCleaner
import logging
import sys
import os

def main():
    print("🏢 Starting LA County Restaurant Data Extraction...")
    print("📍 Coverage: All major cities and areas in Los Angeles County")
    print("🔍 Expected to search ~80+ areas")
    print("-" * 60)
    
    try:
        # Initialize extractor
        extractor = LACountyRestaurantExtractor()
        
        # Extract data for all LA County areas
        restaurants = extractor.run_full_extraction()
        
        if not restaurants:
            print("⚠️  No restaurants found. Check API key and connection.")
            return
        
        # Save to CSV
        df = extractor.save_to_csv()
        
        if df is None:
            print("❌ Failed to save data to CSV")
            return
        
        # Clean and process data
        print("\n📋 Running data cleaning and processing...")
        cleaner = DataCleaner()
        cleaner.process_all_data()
        
        # Final success message with next steps
        print("\n" + "="*60)
        print("✅ EXTRACTION COMPLETE!")
        print("="*60)
        print(f"📊 Total restaurants extracted: {len(df)}")
        print(f"📍 ZIP codes covered: {df['ZIP_Code'].nunique()}")
        print(f"🏘️  Neighborhoods covered: {df['Neighborhood'].nunique()}")
        print(f"⭐ Average rating: {df['Rating'].mean():.2f}")
        
        print("\n📁 Files created:")
        print("   • data/processed/la_county_restaurants_final.csv (for Power BI)")
        print("   • logs/extraction.log (extraction details)")
        print("   • data/raw/*.json (backup data by area)")
        
        print("\n🚀 Next Steps:")
        print("   1. Open Power BI Desktop")
        print("   2. Import 'la_county_restaurants_final.csv'")
        print("   3. Create visualizations using ZIP_Code for geographic mapping")
        print("   4. Use Neighborhood for regional analysis")
        print("   5. Analyze Price_Level and Rating distributions")
        
        print("\n📈 Power BI Visualization Suggestions:")
        print("   • Filled Map: ZIP_Code → Restaurant count (heat map)")
        print("   • Bar Chart: Neighborhood → Average Rating")
        print("   • Pie Chart: Price_Level distribution")
        print("   • Scatter Plot: Rating vs Review_Count by Neighborhood")
        
    except KeyboardInterrupt:
        print("\n⏹️  Extraction cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ FATAL ERROR: {str(e)}")
        logging.error(f"Fatal error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()