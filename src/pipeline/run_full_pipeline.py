# Jack Wilson
# 1/17/2026
# This file runs the full project pipeline, combining scraping, cleaning, merging, and modeling

# --------------------------------------------------------------------------------

# Import modules
import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.scraping.scrape_all import scrape_all


# --------------------------------------------------------------------------------

def main():

    # Scraping
    scrape_all()


if __name__ == "__main__":
    main()