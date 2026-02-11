# Jack Wilson
# 1/17/2026
# This file runs the full project pipeline, combining scraping, merging, cleaning, and modeling

# --------------------------------------------------------------------------------
# Import modules

import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.scraping.scrape_all import scrape_all
from src.cleaning.clean_all import clean_all
from src.modeling.model_all import model_all


# --------------------------------------------------------------------------------

def pipeline():

    # Scraping
    scrape_all(timestamp=True)

    # Cleaning
    clean_all(timestamp=True)

    # Modeling
    model_all(from_file=True, train_new_model=True, save_results=True, verbose=False, timestamp=True)


if __name__ == "__main__":
    pipeline()