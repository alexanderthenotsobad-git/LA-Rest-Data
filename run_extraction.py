#!/usr/bin/env python3
"""
Main script to run LA restaurant data extraction
Usage: python run_extraction.py
"""

from src.data_extractor import RestaurantExtractor
from src.data_cleaner import DataCleaner
import logging

def main():
    print("Starting LA Restaurant Data Extraction...")
    
    # Initialize extractor
    extractor = RestaurantExtractor()
    
    # Extract data for all areas
    extractor.run_full_extraction()
    
    # Clean and process data
    cleaner = DataCleaner()
    cleaner.process_all_data()
    
    print("Extraction complete! Check data/processed/ for results.")

if __name__ == "__main__":
    main()