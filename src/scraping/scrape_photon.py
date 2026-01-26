# Jack Wilson
# 1/10/2026
# This file uses place/city/country to scrape coordinates from Photon API and elevation from Open Elevation API

# --------------------------------------------------------------------------------
# Import modules

import pandas as pd
import os, sys, re

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.utils.utils import get_location_data, print_progress_bar, save_id_map
from src.utils.project_functions import handle_appending, handle_successful_urls, check_new_urls, clean_circuit_name
from src.scraping.scrape_wikipedia import scrape_circuits


# --------------------------------------------------------------------------------
# Location

def scrape_location():
    
    print(f"\nScraping locations...")
    
    # Establish paths
    CIRCUITS_PATH = os.path.join(PROJECT_ROOT, 'data/raw/circuits_raw.csv')
    LOCATIONS_PATH = os.path.join(PROJECT_ROOT, 'data/raw/locations_raw.csv')
    SUCCESSFUL_URLS_TEMP_PATH = os.path.join(PROJECT_ROOT, 'data/raw/successful_urls.pkl')
    SUCCESSFUL_URLS_PATH = os.path.join(PROJECT_ROOT, f'data/successful/successful_urls_locations.pkl')
    
    # Init lists
    location_data = []
    successful_urls = []
    failed_loc, failed_elev, failed_error = [], [], []

    # Load circuit data
    print("   Locating circuit data...")
    if not os.path.exists(CIRCUITS_PATH):
        print("   No circuit data found. Collecting circuit data before continuing...")
        scrape_circuits()
    circuits = pd.read_csv(CIRCUITS_PATH)

    # Create cleaned place column and convert to a list
    circuits['cleaned_place'] = circuits['name'].apply(clean_circuit_name)
    places = circuits['cleaned_place'].tolist()

    # Find new locations
    urls = check_new_urls(places, SUCCESSFUL_URLS_PATH, from_file=False)

    # Filter circuits to only include those with new places
    circuits = circuits[circuits['cleaned_place'].isin(urls)].reset_index(drop=True)
    total_rows = circuits.shape[0]
    print("   Circuit data loaded\n")
  
    # Iterate through each circuit and get location data
    print(f"   Collecting location data for {total_rows} circuits...")
    for idx, row in circuits.iterrows():
        original_name = row['name']
        
        # Clean place name by stripping non-text and excess white space
        place = re.sub(r'[^A-Za-z0-9\s]', '', row['name']).strip()
        city = row['location']
        country = row['country']
        
        # Collect location data
        location = get_location_data(place, city, country)
        if location == 'no_coords':
            failed_loc.append(place)
        elif location == 'no_elev':
            failed_elev.append(place)
        elif location == 'error':
            failed_error.append(place)
        
        # Create location dictionary and append to list
        if isinstance(location, dict):
            location_info = {
                'original_name': original_name,
                'cleaned_name': place,
                'city': city,
                'country': country,
                'latitude': location['latitude'] if location else None,
                'longitude': location['longitude'] if location else None,
                'elevation': location['elevation'] if location else None
            }
            location_data.append(location_info)
            successful_urls.append(place)

        # Update progress bar
        print_progress_bar(idx + 1, total_rows)
    
    # Print any failed locations
    print("\n\n")
    if len(failed_loc) > 0:
        print(f"   Coordinates not found for {len(failed_loc)} locations:")
        for failed in failed_loc:
            print(f"      - {failed}")
    if len(failed_elev) > 0:
        print(f"\n   Elevation not found for {len(failed_elev)} locations:")
        for failed in failed_elev:
            print(f"      - {failed}")
    if len(failed_error) > 0:
        print(f"\n   Errors while searching for {len(failed_error)} locations:")
        for failed in failed_error:
            print(f"      - {failed}")
    print()

    # Handle appending new data or creating new file 
    df = pd.DataFrame(location_data)
    handle_appending(LOCATIONS_PATH, df, title='locations', dup_subset=['cleaned_name'])
    
    # Handle successful url file
    save_id_map(SUCCESSFUL_URLS_TEMP_PATH, successful_urls)
    handle_successful_urls(SUCCESSFUL_URLS_PATH, SUCCESSFUL_URLS_TEMP_PATH)
    
    print(f"Location scraping complete\n")