# Jack Wilson
# 12/4/2025
# This file scrapes all data from the official F1 site

# --------------------------------------------------------------------------------

# Import modules
import pandas as pd
import os, sys
import pickle
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sklearn.linear_model import LinearRegression
from pathlib import Path

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.utils.utils import load_id_map, save_id_map, create_browser, scrape_url_table
from src.utils.project_functions import constructor_mapping, get_date, handle_appending, handle_successful_urls, check_new_urls

data_folder_path = os.path.join(PROJECT_ROOT, 'data/raw')
links_2001_2017_path = os.path.join(PROJECT_ROOT, 'data/raw/links_2001_2017.pkl')
links_2018_path = os.path.join(PROJECT_ROOT, 'data/raw/links_2018+.pkl')

# --------------------------------------------------------------------------------
# Race Links 2001-2017

def scrape_2001_links():
    
    # Check if file already exists
    if os.path.exists(links_2001_2017_path):
        print("\nLinks 2001-2017 already scraped\n")
        return

    print("\nScraping links (2001-2017)...")
    
    # Establish web browser and initial variables
    browser = create_browser()
    year_begin = 2001
    year_end = 2017
    race_urls = []

    while year_begin <= year_end:

        # Use the years to crawl across season pages
        url = "https://www.formula1.com/en/results/" + str(year_begin) + "/races"
        browser.get(url)
        
        table = browser.find_elements(By.TAG_NAME, "table")
        for tr in table:
            rows = tr.find_elements(By.TAG_NAME, "tr")[1:]
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                
                # Url for each specific race
                link = cells[0].find_element(By.TAG_NAME, "a")
                race_urls.append(link.get_attribute("href"))

        year_begin += 1

    browser.close()

    # Save links to file
    print("   Saving links to file...")
    load_id_map(links_2001_2017_path)
    save_id_map(links_2001_2017_path, race_urls)
    print("Link scraping complete\n")


# --------------------------------------------------------------------------------
# Race Results 2001-2017

def scrape_2001_results():

    # Establish paths
    results_2001_path = os.path.join(PROJECT_ROOT, 'data/raw/race_results_raw_2001-2017.csv')
    
    # Check if file already exists
    if os.path.exists(results_2001_path):
        print("\nResults 2001-2017 already scraped\n")
        return

    print("\nScraping results (2001-2017)...")
    
    # Establish variables
    urls = load_id_map(links_2001_2017_path)
    min_col = 7
    max_col = 7
    col_idx_map = {
        'date': get_date,
        'driver_id': 2,
        'team_id': 3,
        'position': 0,
        'driver_name': 2,
        'points': 6}
    id_cols = ['driver_id', 'team_id']
    page_lvl_cols = ['date']

    # Scrape 2001-2017 results
    df = scrape_url_table(
        urls=urls,
        min_col=min_col,
        max_col=max_col,
        col_idx_map=col_idx_map,
        id_cols=id_cols,
        page_lvl_cols=page_lvl_cols,
        id_mask=constructor_mapping)
    print("   Saving results to file...")
    df.to_csv(results_2001_path, encoding='utf-8', index=False)
    print("Result scraping complete\n")


# --------------------------------------------------------------------------------
# Pit Stops 2016-2017

def scrape_2016_pits():
    
    # Establish paths
    pits_2016_path = os.path.join(PROJECT_ROOT, 'data/raw/pit_stop_results_raw_2016-2017.csv')

    # Check if file already exists
    if os.path.exists(pits_2016_path):
        print("\nPit stops 2016-2017 already scraped\n")
        return

    print("\nScraping pit stops (2016-2017)...")

    # Create pit stop URLs
    urls = load_id_map(links_2001_2017_path)
    pit_urls = []
    for url in urls:
        if url.split('/')[5] in ['2016', '2017']:
            ps_url = url.replace('/race-result', '/pit-stop-summary')
            pit_urls.append(ps_url)

    # Establish other variables
    min_col = 8
    max_col = 8
    col_idx_map = {
        'date': get_date,
        'driver_id': 2,
        'team_id': 3,
        'stop_number': 0,
        'stop_lap': 4,
        'pits_time': 6}
    id_cols = ['driver_id', 'team_id']
    page_lvl_cols = ['date']

    # Scrape pit stop results
    df = scrape_url_table(
        urls=pit_urls,
        min_col=min_col,
        max_col=max_col,
        col_idx_map=col_idx_map,
        id_cols=id_cols,
        page_lvl_cols=page_lvl_cols,
        id_mask=constructor_mapping)
    print("   Saving pit stops to file...")
    df.to_csv(pits_2016_path, encoding='utf-8', index=False)
    print("Pit stop scraping complete\n")


# --------------------------------------------------------------------------------
# Race Links 2018+

def scrape_2018_links():
    
    print(f"\nScraping links and rounds (2018+)...")

    # Establish web browser and initial variables
    browser = create_browser()
    year_end = 2018
    year_begin = datetime.now().year
    new = True
    race_urls = []

    # Establish paths
    rounds_path = os.path.join(PROJECT_ROOT, 'data/raw/rounds_raw.csv')

    # Find existing links and rounds
    existing_links = load_id_map(links_2018_path)
    existing_year = None
    existing_round = 0
    
    if os.path.exists(rounds_path) and len(existing_links) > 0:
        print("   Existing links and rounds found...")
        existing_rounds = pd.read_csv(rounds_path)
        last_url = existing_rounds['race_url'].iloc[-1]
        existing_year = int(last_url.split('/')[5])
        existing_round = int(existing_rounds['round_number'].iloc[-1])
        print(f"   Last existing: Year {existing_year}, Round {existing_round}")
        print("   Scraping new links and rounds...")
    elif os.path.exists(rounds_path) and len(existing_links) <= 0:
        print("   Existing rounds found...")
        print("   No existing links found...")
        print("   Scraping all links and rounds...")
    elif len(existing_links) > 0:
        print("   No existing rounds found...")
        print("   Existing links found...")
        print("   Scraping all links and rounds...")
    else:
        print("   No existing links or rounds found...")
        print("   Scraping all links and rounds...")

    while new == True:
        
        # If the scrape year goes below 2018, break
        if year_begin < year_end:
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
                    print(f"   Found existing link, stopping scrape")
                    new = False
                    break
                race_urls.append(link)
            
            if new == False:
                break
        if new == False:
            break

        year_begin -= 1

    browser.close()

    # Reverse the order of race_urls
    race_urls.reverse()
    
    print(f"\n   Scraped {len(race_urls)} new races")
    if race_urls:
        print("   Sample URLs:")
        for url in race_urls[:3]:
            print(f"      {url}")
        print(f"      ...\n")
    
    # Assign round numbers based on year of race
    round_number = []
    current_year = None
    current_round = 0
    
    for url in race_urls:
        race_year = int(url.split('/')[5])
        
        # Decide what round to append
        if existing_year is not None and race_year == existing_year:
            existing_round += 1
            round_number.append(existing_round)
            current_year = race_year
            current_round = existing_round
            existing_year = None 
        elif race_year != current_year:
            current_year = race_year
            current_round = 1
            round_number.append(current_round)
        else:
            current_round += 1
            round_number.append(current_round)

    # Convert link data to dataframe
    link_data = pd.DataFrame({'race_url': race_urls, 'round_number': round_number})
    
    # Check if rounds_raw.csv exists and append if it does
    if os.path.exists(rounds_path) and len(race_urls) > 0:
        print("   Appending new rounds to existing file...")
        existing_rounds = pd.read_csv(rounds_path)
        combined_rounds = pd.concat([existing_rounds, link_data], ignore_index=True)
        combined_rounds.to_csv(rounds_path, encoding='utf-8', index=False)
    elif len(race_urls) > 0:
        print("   Creating new rounds file...")
        link_data.to_csv(rounds_path, encoding='utf-8', index=False)
    else:
        print("   No new rounds to save")

    # Check if links_2018+.pkl exists and append if it does
    if os.path.exists(links_2018_path) and len(race_urls) > 0:
        print("   Appending new links to existing file...")
        existing_links = load_id_map(links_2018_path)
        combined_links = existing_links + race_urls
        save_id_map(links_2018_path, combined_links)
    elif len(race_urls) > 0:
        print("   Creating new links file...")
        save_id_map(links_2018_path, race_urls)
    else:
        print("   No new links to save")

    print(f"Link and round scraping complete\n")


# --------------------------------------------------------------------------------
# Race Results 2018+

def scrape_2018_results():
    
    # Establish title
    title = 'results'
    title_cap = title.capitalize()
    print(f"\nScraping {title} (2018+)...")
    
    # Establish paths
    successful_urls_temp_path = os.path.join(PROJECT_ROOT, 'data/raw/successful_urls.pkl')
    successful_urls_path = os.path.join(PROJECT_ROOT, f'data/raw/successful_urls_{title}.pkl')
    results_2018_path = os.path.join(PROJECT_ROOT, 'data/raw/race_results_raw_2018+.csv')
    
    # Check for new URLs
    urls = check_new_urls(links_2018_path, successful_urls_path)
    if len(urls) == 0:
        print(f"{title_cap} scraping complete\n")
        return
    print(f"   Found {len(urls)} new links...")

    # Establish variables
    min_col = 7
    max_col = 7
    col_idx_map = {
        'race_id': lambda browser: browser.find_element(By.ID, "content-dropdown").text + '_' + browser.current_url.split("/")[5],
        'driver_id': 2,
        'circuit_id': lambda browser: browser.find_element(By.ID, "content-dropdown").text,
        'team_id': 3,
        'year': lambda browser: int(browser.current_url.split("/")[5]),
        'race_url': lambda browser: browser.current_url,
        'circuit_name': lambda browser: browser.find_element(By.ID, "content-dropdown").text,
        'driver_name': 2,
        'team_name': 3,
        'end_position': 0,
        'points': 6,
        'laps_completed': 4}
    id_cols = ['race_id', 'driver_id', 'circuit_id', 'team_id']
    page_lvl_cols = ['race_id', 'circuit_id', 'year', 'race_url', 'circuit_name']

    # Scrape 2018+ results
    print(f"   Scraping {title}...")
    df = scrape_url_table(
        urls=urls,
        min_col=min_col,
        max_col=max_col,
        col_idx_map=col_idx_map,
        data_folder=data_folder_path,
        id_cols=id_cols,
        page_lvl_cols=page_lvl_cols,
        id_mask=constructor_mapping,
        save_successful_urls=True)

    # Handle appending new data or creating new file 
    handle_appending(results_2018_path, df, title)
    
    # Handle successful url file
    handle_successful_urls(successful_urls_path, successful_urls_temp_path)
    
    print(f"{title_cap} scraping complete\n")


# --------------------------------------------------------------------------------
# Practices 2018+

def scrape_2018_practices():
    
    # Establish title
    title = 'practices'
    title_cap = title.capitalize()
    print(f"\nScraping {title} (2018+)...")
    
    # Establish paths
    successful_urls_temp_path = os.path.join(PROJECT_ROOT, 'data/raw/successful_urls.pkl')
    successful_urls_path = os.path.join(PROJECT_ROOT, f'data/raw/successful_urls_{title}.pkl')
    practices_2018_path = os.path.join(PROJECT_ROOT, 'data/raw/practice_results_raw.csv')
    
    # Load race URLs
    urls = load_id_map(links_2018_path)
    
    # Create practice URLs
    practice_urls = []
    for url in urls:
        emilia_romagna_2020 = url.split('/')[5] == '2020' and url.split('/')[8] == 'emilia-romagna'
        practice_nums = [0] if emilia_romagna_2020 else [1, 2, 3]
        for practice_num in practice_nums:
            practice_url = url.replace('/race-result', f'/practice/{practice_num}')
            practice_urls.append(practice_url)
    
    # Check for new URLs
    new_urls = check_new_urls(practice_urls, successful_urls_path, from_file=False)
    if len(new_urls) == 0:
        print(f"{title_cap} scraping complete\n")
        return
    print(f"   Found {len(new_urls)} new links...")

    # Establish variables
    min_col = 6
    max_col = 6
    col_idx_map = {
        'race_id': lambda browser: browser.find_element(By.ID, "content-dropdown").text + '_' + browser.current_url.split("/")[5],
        'driver_id': 2,
        'team_id': 3,
        'session_type': lambda browser: browser.current_url.split("/")[9] + browser.current_url.split("/")[10],
        'lap_time': 4,
        'lap_count': 5,
        'position': 0}
    id_cols = ['race_id', 'driver_id', 'team_id']
    page_lvl_cols = ['race_id', 'session_type']

    # Scrape practice results
    print(f"   Scraping {title}...")
    df = scrape_url_table(
        urls=new_urls,
        min_col=min_col,
        max_col=max_col,
        col_idx_map=col_idx_map,
        data_folder=data_folder_path,
        id_cols=id_cols,
        page_lvl_cols=page_lvl_cols,
        id_mask=constructor_mapping,
        save_successful_urls=True)
    
    # Handle appending new data or creating new file
    handle_appending(practices_2018_path, df, title)
        
    # Handle successful url file
    handle_successful_urls(successful_urls_path, successful_urls_temp_path)
    
    print(f"{title_cap} scraping complete\n")


# --------------------------------------------------------------------------------
# Qualifying 2018+

def scrape_2018_qualifying():
    
    # Establish title
    title = 'qualifying'
    title_cap = title.capitalize()
    print(f"\nScraping {title} (2018+)...")
    
    # Establish paths
    successful_urls_temp_path = os.path.join(PROJECT_ROOT, 'data/raw/successful_urls.pkl')
    successful_urls_path = os.path.join(PROJECT_ROOT, f'data/raw/successful_urls_{title}.pkl')
    qualifying_2018_path = os.path.join(PROJECT_ROOT, 'data/raw/qualifying_results_raw.csv')
    
    # Load race URLs
    urls = load_id_map(links_2018_path)
    
    # Create practice URLs
    qualifying_urls = []
    for url in urls:
        qual_url = url.replace('/race-result', '/qualifying')
        qualifying_urls.append(qual_url)
    
    # Check for new URLs
    new_urls = check_new_urls(qualifying_urls, successful_urls_path, from_file=False)
    if len(new_urls) == 0:
        print(f"{title_cap} scraping complete\n")
        return
    print(f"   Found {len(new_urls)} new links...")

    # Establish variables
    min_col = 8
    max_col = 8
    col_idx_map = {
        'race_id': lambda browser: browser.find_element(By.ID, "content-dropdown").text + '_' + browser.current_url.split("/")[5],
        'driver_id': 2,
        'team_id': 3,
        'q1_time': 4,
        'q2_time': 5,
        'q3_time': 6,
        'qual_position': 0,
        'qual_laps': 7}
    id_cols = ['race_id', 'driver_id', 'team_id']
    page_lvl_cols = ['race_id']

    # Scrape qualifying results
    print(f"   Scraping {title}...")
    df = scrape_url_table(
        urls=new_urls,
        min_col=min_col,
        max_col=max_col,
        col_idx_map=col_idx_map,
        data_folder=data_folder_path,
        id_cols=id_cols,
        page_lvl_cols=page_lvl_cols,
        id_mask=constructor_mapping,
        save_successful_urls=True)
    
    # Handle appending new data or creating new file
    handle_appending(qualifying_2018_path, df, title)
        
    # Handle successful url file
    handle_successful_urls(successful_urls_path, successful_urls_temp_path)
    
    print(f"{title_cap} scraping complete\n")


# --------------------------------------------------------------------------------
# Starting Grid 2018+

def scrape_2018_starting_grid():
    
    # Establish title
    title = 'starting grid'
    title_cap = title.capitalize()
    title_file = 'starting_grid'
    print(f"\nScraping {title} (2018+)...")
    
    # Establish paths
    successful_urls_temp_path = os.path.join(PROJECT_ROOT, 'data/raw/successful_urls.pkl')
    successful_urls_path = os.path.join(PROJECT_ROOT, f'data/raw/successful_urls_{title_file}.pkl')
    starting_grid_2018_path = os.path.join(PROJECT_ROOT, 'data/raw/starting_grid_results_raw.csv')
    
    # Load race URLs
    urls = load_id_map(links_2018_path)
    
    # Create starting grid URLs
    starting_urls = []
    for url in urls:
        start_url = url.replace('/race-result', '/starting-grid')
        starting_urls.append(start_url)
    
    # Check for new URLs
    new_urls = check_new_urls(starting_urls, successful_urls_path, from_file=False)
    if len(new_urls) == 0:
        print(f"{title_cap} scraping complete\n")
        return
    print(f"   Found {len(new_urls)} new links...")

    # Establish variables
    min_col = 4
    max_col = 5
    col_idx_map = {
        'race_id': lambda browser: browser.find_element(By.ID, "content-dropdown").text + '_' + browser.current_url.split("/")[5],
        'driver_id': 2,
        'team_id': 3,
        'start_position': 0}
    id_cols = ['race_id', 'driver_id', 'team_id']
    page_lvl_cols = ['race_id']

    # Scrape practice results
    print(f"   Scraping {title}...")
    df = scrape_url_table(
        urls=new_urls,
        min_col=min_col,
        max_col=max_col,
        col_idx_map=col_idx_map,
        data_folder=data_folder_path,
        id_cols=id_cols,
        page_lvl_cols=page_lvl_cols,
        id_mask=constructor_mapping,
        save_successful_urls=True)
    
    # Handle appending new data or creating new file
    handle_appending(starting_grid_2018_path, df, title)
        
    # Handle successful url file
    handle_successful_urls(successful_urls_path, successful_urls_temp_path)
    
    print(f"{title_cap} scraping complete\n")