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
from src.scraping.scrape_f1_site import *
from src.scraping.scrape_fastf1 import *
from src.scraping.scrape_wikipedia import *
from src.scraping.scrape_photon import *


# --------------------------------------------------------------------------------

def scrape_all():

    # F1 Site 2001-2017
    scrape_2001_links()
    scrape_2001_results()
    scrape_2016_pits()

    # F1 Site 2018+
    scrape_2018_links()
    scrape_2018_results()
    scrape_2018_practices()
    scrape_2018_qualifying()
    scrape_2018_starting_grid()
    scrape_2018_pit_stops()
    scrape_2018_fastest_laps()
    scrape_2018_driver_codes()
    
    # FastF1
    collect_fastf1_data()
    aggregate_laps()
    aggregate_weather()
    aggregate_flags()

    # Wikipedia
    scrape_circuits()

    # Photon
    scrape_location()


if __name__ == "__main__":
    scrape_all()