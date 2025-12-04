# Jack Wilson
# 12/1/2025
# This file scrapes and cleans new Formula 1 result data

# Import modules
import pandas as pd
import os, sys
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(current_dir)
SRC_DIR = os.path.join(PROJECT_ROOT, 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)
from utils.data_functions import load_id_map, save_id_map, scrape_url_table, constructor_mapping  # type: ignore

# Establish web browser and initial variables
browser = webdriver.Chrome()
browser.maximize_window()
year_begin = datetime.now().year
new_race_urls = []
new = True

existing_links = load_id_map(os.path.join(PROJECT_ROOT, 'data/raw/links_2018+.pkl'))

while new == True:
    
    # If the scrape year goes below 2018, break
    if year_begin < 2018:
        break
    
    # Use the years to crawl across season pages
    url = "https://www.formula1.com/en/results/" + str(year_begin) + "/races"
    browser.get(url)
    
    table = browser.find_elements(By.TAG_NAME, "table")
    for tr in table:
        rows = tr.find_elements(By.TAG_NAME, "tr")[1:]
        for row in reversed(rows):
            cells = row.find_elements(By.TAG_NAME, "td")
            
            # Url for each specific race
            link = cells[0].find_element(By.TAG_NAME, "a").get_attribute("href")

            # If the link is new append it, otherwise break
            if link in existing_links:
                new = False
                break
            new_race_urls.append(link)

    year_begin -= 1

browser.close()

# Print new links
for x in new_race_urls:
    print(x)

# Merge the new links with the existing ones and save
new_race_urls.reverse()
current_links = existing_links + new_race_urls
save_id_map(os.path.join(PROJECT_ROOT, 'data/raw/links_2018+_new.pkl'), current_links)  # Change this to overwrite later
