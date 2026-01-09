# Jack Wilson
# 12/4/2025
# This file scrapes all project data from scratch

# --------------------------------------------------------------------------------

# Import modules
import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.utils.utils import compare_data_files
from src.scraping.scrape_f1_site import scrape_2001_links, scrape_2001_results, scrape_2016_pits
from src.scraping.scrape_f1_site import scrape_2018_links, scrape_2018_results, scrape_2018_practices, scrape_2018_qualifying, scrape_2018_starting_grid, scrape_2018_pit_stops, scrape_2018_fastest_laps, scrape_2018_driver_codes
from src.scraping.scrape_fastf1 import collect_fastf1_data, aggregate_laps

# --------------------------------------------------------------------------------

def main():

    # F1 Site 2001-2017
    # scrape_2001_links()
    # scrape_2001_results()
    # scrape_2016_pits()

    # F1 Site 2018+
    # scrape_2018_links()
    # scrape_2018_results()
    # scrape_2018_practices()
    # scrape_2018_qualifying()
    # scrape_2018_starting_grid()
    # scrape_2018_pit_stops()
    # scrape_2018_fastest_laps()
    # scrape_2018_driver_codes()
    
    # FastF1
    # collect_fastf1_data()
    aggregate_laps()


if __name__ == "__main__":
    main()


# --------------------------------------------------------------------------------

# Compare race results files
# original_file = os.path.join(PROJECT_ROOT, 'data/raw/race_results_raw_2018+.csv')
# temp_file = os.path.join(PROJECT_ROOT, 'data/raw/race_results_raw_2018+_TEMP.csv')
# compare_data_files(original_file, temp_file)


