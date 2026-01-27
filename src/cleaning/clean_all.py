# Jack Wilson
# 1/19/2026
# This file cleans and merges all raw data into its final form

# --------------------------------------------------------------------------------
# Import modules

import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.cleaning.clean_raw import *
from src.cleaning.merge import *


# --------------------------------------------------------------------------------

def clean_all():
    
    # Clean raw
    print("\nCleaning raw data...")
    clean_id_map()
    clean_results_2001()
    clean_results_2018()
    clean_practices_2018()
    clean_qualifying_2018()
    clean_starting_grid_2018()
    clean_pit_stops_2018()
    clean_laps()
    clean_weather()
    clean_flags()
    clean_circuits()
    clean_locations()
    print("Raw data cleaned\n")

    # Merge
    print("\nMerging data...")
    neutral_merge()
    pre_qual_merge()
    pre_race_merge()
    print("Data merged\n")


if __name__ == "__main__":
    clean_all()