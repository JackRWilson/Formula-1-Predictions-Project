# Jack Wilson
# 10/15/2025
# This file defines general functions

# ==============================================================================================
# Import Modules
# ==============================================================================================

import pandas as pd
import numpy as np
import os, time, tempfile, shutil
import pickle
import requests
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from urllib.parse import quote


# ==============================================================================================
# I. Load/Save ID Maps
# ==============================================================================================

def load_id_map(path: str, default: dict | list | None = None):
    """
    Load the pickle file ID maps if they exist, otherwise return an empty dictionary or list
    
    """
    if os.path.exists(path):
        with open(path, 'rb') as f:
            return pickle.load(f)
    else:
        return {} if default is None else default


def is_file_locked(filepath: str) -> bool:
    """
    Check if a file is currently locked by another process
    
    """
    try:
        with open(filepath, 'r+b') as f:
            pass
        return False
    except (IOError, OSError):
        return True


def save_id_map(path: str, id_map, max_retries: int = 3):
    """
    Save the ID map to a pickle file with retry mechanism for permission errors

    """
    # Check if file is locked before attempting to save
    if os.path.exists(path) and is_file_locked(path):
        print(f"Warning: {path} appears to be locked by another process")
    
    for attempt in range(max_retries):
        try:
            # Try to save directly first
            with open(path, 'wb') as f:
                pickle.dump(id_map, f)
            return  # Success, exit function
            
        except PermissionError as e:
            if attempt < max_retries - 1:
                print(f"Permission denied on attempt {attempt + 1}, retrying in 1 second...")
                time.sleep(1)
            else:
                # Final attempt: try using a temporary file and then moving it
                try:
                    temp_dir = os.path.dirname(path)
                    temp_file = tempfile.NamedTemporaryFile(mode='wb', dir=temp_dir, delete=False, suffix='.pkl')
                    
                    with temp_file as f:
                        pickle.dump(id_map, f)
                    
                    # Move the temporary file to the target location
                    shutil.move(temp_file.name, path)
                    print(f"Successfully saved {path} using temporary file method")
                    return
                    
                except Exception as final_e:
                    print(f"Failed to save {path} after {max_retries} attempts: {final_e}")
                    raise final_e
        except Exception as e:
            print(f"Unexpected error saving {path}: {e}")
            raise e


# ==============================================================================================
# II. Column:Index Mapping
# ==============================================================================================

def init_col_map(col_map: dict):
    """
    Takes a {column_name: column_index} dictionary as an input and returns a new dictionary with
    indexes and empty lists

    """
    return {col: {'index': index, 'values': []} for col, index in col_map.items()}


# ==============================================================================================
# III. Scrape Data from URL
# ==============================================================================================

def create_browser():
    """
    Creates a Chrome browser with logging suppressed

    """
    chrome_options = Options()
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    browser = webdriver.Chrome(options=chrome_options)
    browser.maximize_window()
    return browser


_progress_start_time = None
def print_progress_bar(current, total, bar_length=40, start_time=None):
    """
    Print a progress bar with ETA that updates on the same lines
    
    """
    # Init start time
    global _progress_start_time
    if start_time is not None:
        _progress_start_time = start_time
    elif _progress_start_time is None or current == 1:
        _progress_start_time = time.time()
    
    percent = current * 100 // total
    filled = bar_length * current // total
    bar = '█' * filled + '-' * (bar_length - filled)
    
    # Calculate ETA
    eta_str = "Calculating..."
    if _progress_start_time and current > 0:
        elapsed = time.time() - _progress_start_time
        if elapsed > 0:
            rate = current / elapsed
            if rate > 0:
                remaining = (total - current) / rate
        
                # Format time nicely
                if remaining < 60:
                    eta_str = f"{int(remaining)}s"
                elif remaining < 3600:
                    mins = int(remaining // 60)
                    secs = int(remaining % 60)
                    eta_str = f"{mins}m {secs}s"
                else:
                    hours = int(remaining // 3600)
                    mins = int((remaining % 3600) // 60)
                    eta_str = f"{hours}h {mins}m"

    # Print progress bar and ETA
    print(f"\r   Progress: [{bar}] {current}/{total} ({percent}%)", end='')
    print(f"\n   ETA: {eta_str}            ", end='', flush=True)
    print("\033[A", end='', flush=True)

    # Reset on completion
    if current == total:
        _progress_start_time = None


def scrape_url_table(
    urls: list,
    min_col: int,
    max_col: int,
    col_idx_map: dict,
    data_folder: str,
    id_cols: list = None,
    page_lvl_cols: list = None,
    id_mask: dict = None,
    auto_url_id: bool = False,
    save_successful_urls: bool = False
    ) -> pd.DataFrame:
    """
    Scrapes a table from a website and returns a dataframe of scraped values


    Parameters
    ----------
    urls : list
        The webpage URL(s) to scrape
    min_col : int
        Minimum number of columns to accept in a table row
    max_col : int
        Maximum number of columns to accept in a table row
    col_idx_map : dict
        A dictionary mapping desired column names to column indices
        Example: {'race_id': None, 'start_pos': 1, 'driver_name': 3...}
    id_cols : list, optional
        List of the names of ID columns in the col_idx_map
    page_lvl_cols : list, optional
        List of columns that need scraping on the page level, index will
        contain path to scrape that data
    data_folder : str
        File path of data folder for saving any ID maps and successful URLs
    id_mask : dict, optional
        Dictionary mapping column names to value mapping dictionaries
        Example: {'team_name': {'Red Bull Racing': 'Red Bull'}}
    auto_url_id : bool, optional
        Whether to automatically create URL IDs for each row
        Default: False
    save_successful_urls : bool, optional
        Whether to save a list of successfully scraped URLs to the data folder
        Default: False
    

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the scraped table
    
    """
    # Validate min_col and max_col
    if min_col > max_col:
        raise ValueError(f"min_col ({min_col}) cannot be greater than max_col ({max_col})")
    
    # Initiate data mapping
    col_data = init_col_map(col_idx_map)

    # Load URL ID map only if url_id is True
    if auto_url_id:
        if data_folder:
            url_id_map = load_id_map(f'{data_folder}/url_id_map.pkl')
        else:
            url_id_map = load_id_map('url_id_map.pkl')

    # Initialize list for successful URLs if saving is enabled
    successful_urls = [] if save_successful_urls else None

    # Establish web browser
    browser = create_browser()
    
    total_urls = len(urls)
    print(f"\n   Scraping {total_urls} URLs...")

    for i, url in enumerate(urls, start=1):
        
        # Update progress bar
        print_progress_bar(i, total_urls)
        
        # Validate URL
        try:
            browser.get(url)
            time.sleep(0.5)
        except Exception as e:
            print(f'\nURL ERROR: "{url}"\n{e}')
            continue

        # Get or create URL ID only if auto_url_id is True
        if auto_url_id:
            if url in url_id_map:
                url_id_val = url_id_map[url]
            else:
                url_id_val = max(url_id_map.values()) + 1 if url_id_map else 1
                url_id_map[url] = url_id_val
                
                # Save the updated URL ID map
                if data_folder:
                    save_id_map(f'{data_folder}/url_id_map.pkl', url_id_map)
                else:
                    save_id_map('url_id_map.pkl', url_id_map)

        try:
            # Get page-level data once per URL
            page_level_data = {}
            if page_lvl_cols:
                for col_name in page_lvl_cols:
                    if col_name in col_data and callable(col_data[col_name]['index']):
                        page_level_data[col_name] = col_data[col_name]['index'](browser)

            # Find table data
            table = browser.find_elements(By.TAG_NAME, 'table')
            for tr in table:
                rows = tr.find_elements(By.TAG_NAME, 'tr')[1:]
                for row in rows:
                    # Re-find cells for each row to avoid stale element references
                    try:
                        cells = row.find_elements(By.TAG_NAME, 'td')
                    except Exception as e:
                        # If row becomes stale, skip it
                        continue

                    # Validate table has the right number of columns within the specified range
                    num_cells = len(cells)
                    if min_col <= num_cells <= max_col:
                        
                        # Extract all cell texts immediately to avoid stale references
                        cell_texts = []
                        for cell in cells:
                            try:
                                cell_texts.append(cell.text.strip())
                            except Exception as e:
                                # If cell becomes stale, append None
                                cell_texts.append(None)
                        
                        # For each column in the column map append the corresponding data
                        for col_name, col_info in col_data.items():
                            
                            # Skip indexes with None
                            if col_info['index'] == None:
                                continue
                            
                            # Create IDs and ID maps only if id_cols is provided and column is in id_cols
                            if id_cols and col_name in id_cols:
                                
                                # Load or create ID map
                                if data_folder:
                                    id_map = load_id_map(f'{data_folder}/{col_name}_map.pkl')
                                else:
                                    id_map = load_id_map(f'{col_name}_map.pkl')
                                
                                # Get the value from the extracted cell texts using the index from col_map
                                if isinstance(col_info['index'], int):
                                    if col_info['index'] < num_cells:
                                        scraped_value = cell_texts[col_info['index']]
                                    else:
                                        scraped_value = None 
                                elif page_lvl_cols and col_name in page_lvl_cols:
                                    scraped_value = page_level_data[col_name]
                                else:
                                    raise ValueError(f"Unsupported index type for {col_name}: {type(col_info['index'])}")

                                # Apply ID mask if provided
                                if id_mask and col_name in id_mask and scraped_value is not None:
                                    scraped_value = id_mask[col_name].get(scraped_value, scraped_value)
                                
                                # Search through ID map keys to find a match
                                matched_key = None
                                if scraped_value is not None:
                                    for existing_key in id_map.keys():
                                        if scraped_value in existing_key:
                                            matched_key = existing_key
                                            break
                                
                                # Use matched key if found, otherwise use scraped value
                                lookup_key = matched_key if matched_key is not None else scraped_value
                                
                                # Append existing ID or create new key-value pair
                                if scraped_value is None:
                                    col_info['values'].append(None)
                                elif lookup_key in id_map:
                                    col_info['values'].append(id_map[lookup_key])
                                else:
                                    new_id = max(id_map.values()) + 1 if id_map else 1
                                    id_map[lookup_key] = new_id
                                    col_info['values'].append(new_id)
                                    
                                    # Save the updated ID map
                                    if data_folder:
                                        save_id_map(f'{data_folder}/{col_name}_map.pkl', id_map)
                                    else:
                                        save_id_map(f'{col_name}_map.pkl', id_map)
                            
                            # Handle non-ID columns
                            else:
                                if isinstance(col_info['index'], int):
                                    if col_info['index'] < num_cells:
                                        scraped_value = cell_texts[col_info['index']]
                                    else:
                                        scraped_value = None  # Index out of bounds
                                elif page_lvl_cols and col_name in page_lvl_cols:
                                    scraped_value = page_level_data[col_name]
                                else:
                                    raise ValueError(f"Unsupported index type for {col_name}: {type(col_info['index'])}")
                                col_info['values'].append(scraped_value)
                        
                        # Append the same URL ID for every row from this URL only if auto_url_id is True
                        if auto_url_id:
                            if 'url_id' not in col_data:
                                col_data['url_id'] = {'index': None, 'values': []}
                            col_data['url_id']['values'].append(url_id_val)
                                
            # Add URL to successful URLs list if data was found and saving is enabled
            if save_successful_urls:
                successful_urls.append(url)
                                
        except Exception as e:
            print(f'\nURL: {url}')
            print(f'NO DATA FOUND ERROR: {e}')
    
    print()
    browser.close()
    print()
    
    # Save successful URLs to file if enabled
    if save_successful_urls and successful_urls:
        try:
            if data_folder:
                successful_urls_path = f'{data_folder}/successful_urls.pkl'
            else:
                successful_urls_path = 'successful_urls.pkl'
            save_id_map(successful_urls_path, successful_urls)
        except Exception as e:
            print(f"Failed to save successful URLs: {e}")
    
    # Convert column data to DataFrame
    df_data = {}
    for col_name, col_info in col_data.items():
        df_data[col_name] = col_info['values']
    
    try:
        df = pd.DataFrame(df_data)
    except Exception as e:
        print(f'ARRAY LENGTH ERROR: {e}')
        return(f'ERROR: {e}')
    
    return df


# ==============================================================================================
# IV. Aggregate Column Values
# ==============================================================================================

def aggregate_columns(df, columns: list = None, boolean_columns: list = None, string_columns: list = None):
    """
    Universal aggregation function that returns mean, min, max, and std values for numeric columns,
    boolean aggregation for True/False columns, and the first value for string columns.
    
    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame
    columns : list
        List of numeric column names to aggregate. If None, aggregates all numeric columns.
    boolean_columns : list
        List of boolean column names to check for any True values.
    string_columns : list
        List of string column names to extract (just takes first value).
    
    Returns
    -------
    pd.DataFrame
        Aggregated statistics for the specified columns with a single row

    """
    agg = {}
    
    # Handle numeric columns
    if columns is None:
        # Get all numeric columns if none specified
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    for col in columns:
        if col in df.columns:
            agg[f'{col}_mean'] = df[col].mean()
            agg[f'{col}_min'] = df[col].min()
            agg[f'{col}_max'] = df[col].max()
            agg[f'{col}_std'] = df[col].std()
    
    # Handle boolean columns
    if boolean_columns is not None:
        for col in boolean_columns:
            if col in df.columns:
                agg[f'{col}_any'] = bool(df[col].any())
                agg[f'{col}_mean'] = df[col].mean()
    
    # Handle string columns
    if string_columns is not None:
        for col in string_columns:
            if col in df.columns:
                agg[col] = df[col].iloc[0]

    return pd.DataFrame([agg])


# ==============================================================================================
# VI. Get Location Data
# ==============================================================================================

def get_location_data(place, city, country):
    """
    Get latitude, longitude, and elevation for a location.

    Parameters
    ----------
    place : str
        The name of the place/landmark
    city : str
        The city name
    country : str
        The country name

    Returns
    -------
    dict
        A dictionary with keys 'latitude', 'longitude', and 'elevation'
        Returns 'no_coords' if location is not found
        Returns 'no_elevation' if elevation is not found
        Returns 'error' if an error occurs
    
    """
    try:
        # Combine inputs into a single query string
        query = f"{place}, {city}, {country}"

        # Get coordinates from Photon API
        url = f"https://photon.komoot.io/api/?q={query}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        }
        response_raw = requests.get(url, headers=headers)
        if response_raw.status_code != 200:
            return 'error'
        try:
            response = response_raw.json()
        except ValueError:
            return 'error'

        if not response.get('features'):
            return 'no_coords'

        coords = response['features'][0].get('geometry', {}).get('coordinates')
        if not coords or len(coords) < 2:
            return 'no_coords'
        lat, lon = coords[1], coords[0]

        # Get elevation from Open Elevation API
        elevation_url = f"https://api.open-elevation.com/api/v1/lookup?locations={lat},{lon}"
        elevation_raw = requests.get(elevation_url)
        if elevation_raw.status_code != 200:
            return 'error'
        try:
            elevation_response = elevation_raw.json()
        except ValueError:
            return 'error'
        try:
            elevation = elevation_response['results'][0]['elevation']
        except (KeyError, IndexError, TypeError):
            return 'no_elevation'

        return {
            'latitude': lat,
            'longitude': lon,
            'elevation': elevation
        }

    except requests.exceptions.RequestException:
        return 'error'
    except (KeyError, IndexError):
        return 'error'