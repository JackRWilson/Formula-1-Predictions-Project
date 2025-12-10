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
from src.scraping.scrape_f1_site import scrape_2018_links

# --------------------------------------------------------------------------------

# Testing functions
scrape_2018_links()
