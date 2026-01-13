# Jack Wilson
# 1/9/2026
# This file scrapes track data from Wikipedia

# --------------------------------------------------------------------------------
# Import modules

import pandas as pd
import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.utils.utils import scrape_url_table

DATA_FOLDER_PATH = os.path.join(PROJECT_ROOT, 'data/raw')


# --------------------------------------------------------------------------------
# Circuits

def scrape_circuits():
    
    print(f"\nScraping circuits...")
    
    # Establish path
    CIRCUITS_PATH = os.path.join(PROJECT_ROOT, 'data/raw/circuits_raw.csv')
    
    # Load circuits URL
    urls = ["https://en.wikipedia.org/wiki/List_of_Formula_One_circuits"]

    # Establish variables
    min_col = 11
    max_col = 11
    col_idx_map = {
        'name': 0,
        'type': 2,
        'direction': 3,
        'location': 4,
        'country': 5,
        'length': 6,
        'turns': 7,
        'gp': 8,
        'seasons': 9,
        'gps_held': 10}

    # Scrape results
    df = scrape_url_table(
        urls,
        min_col,
        max_col,
        col_idx_map,
        data_folder=DATA_FOLDER_PATH)
    
    # Save to csv
    df.to_csv(CIRCUITS_PATH, encoding='utf-8', index=False)
    print(f"\nCircuit scraping complete\n")