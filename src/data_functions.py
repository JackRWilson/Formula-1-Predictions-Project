# Jack Wilson
# 10/15/2025
# This file defines all functions to be used in other notebooks/scripts

# ==============================================================================================
# Import Modules
# ==============================================================================================

import pandas as pd
import numpy as np
import os, time, tempfile, shutil
import pickle
from selenium import webdriver
from selenium.webdriver.common.by import By


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

def scrape_url_table(urls: list, total_col: int, col_idx_map: dict, id_cols: list, page_lvl_cols: list = None, data_folder: str = '../data/raw', id_mask: dict = None, auto_url_id: bool = False) -> pd.DataFrame:
    """
    Scrapes a table from a website and returns a dataframe of scraped values


    Parameters
    ----------
    urls : list
        The webpage URL(s) to scrape
    total_cols : int
        Number of columns in the table
    col_idx_map : dict
        A dictionary mapping desired column names to column indices
        Example: {'race_id': None, 'start_pos': 1, 'driver_name': 3...}
    id_cols : list
        List of the names of ID columns in the col_idx_map
    page_lvl_cols : list, optional
        List of columns that need scraping on the page level, index will
        contain path to scrape that data
    data_folder : str, optional
        File path of data folder for saving any ID maps
        Default: ../data/raw
    id_mask : dict, optional
        Dictionary mapping column names to value mapping dictionaries
        Example: {'team_name': {'Red Bull Racing': 'Red Bull'}}
    auto_url_id : bool, optional
        Whether to automatically create URL IDs for each row
        Default: False
    

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the scraped table
    
    """
    # Initiate data mapping
    col_data = init_col_map(col_idx_map)

    # Load URL ID map only if url_id is True
    if auto_url_id:
        if data_folder:
            url_id_map = load_id_map(f'{data_folder}/url_id_map.pkl')
        else:
            url_id_map = load_id_map('url_id_map.pkl')

    # Establish web browser
    browser = webdriver.Chrome()
    browser.maximize_window()
    
    for url in urls:
        
        # Validate URL
        try:
            browser.get(url)
        except Exception as e:
            print(f'URL ERROR: "{url}"\n{e}')
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
            # Find table data
            table = browser.find_elements(By.TAG_NAME, 'table')
            for tr in table:
                rows = tr.find_elements(By.TAG_NAME, 'tr')[1:]
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    
                    # Validate table has the right number of columns
                    if len(cells) == total_col:
                        
                        # For each column in the column map append the corresponding data
                        for col_name, col_info in col_data.items():
                            
                            # Skip indexes with None
                            if col_info['index'] == None:
                                continue
                            
                            # Create IDs and ID maps
                            if col_name in id_cols:
                                
                                # Load or create ID map
                                if data_folder:
                                    id_map = load_id_map(f'{data_folder}/{col_name}_map.pkl')
                                else:
                                    id_map = load_id_map(f'{col_name}_map.pkl')
                                
                                # Get the value from the table cell using the index from col_map
                                if isinstance(col_info['index'], int):
                                    scraped_value = cells[col_info['index']].text.strip()
                                elif page_lvl_cols and col_name in page_lvl_cols:
                                    scraped_value = col_info['index'](browser)
                                else:
                                    raise ValueError(f"Unsupported index type for {col_name}: {type(col_info['index'])}")

                                # Apply ID mask if provided
                                if id_mask and col_name in id_mask:
                                    scraped_value = id_mask[col_name].get(scraped_value, scraped_value)
                                
                                # Search through ID map keys to find a match
                                matched_key = None
                                for existing_key in id_map.keys():
                                    if scraped_value in existing_key:
                                        matched_key = existing_key
                                        break
                                
                                # Use matched key if found, otherwise use scraped value
                                lookup_key = matched_key if matched_key is not None else scraped_value
                                
                                # Append existing ID or create new key-value pair
                                if lookup_key in id_map:
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
                                    scraped_value = cells[col_info['index']].text.strip()
                                elif page_lvl_cols and col_name in page_lvl_cols:
                                    scraped_value = col_info['index'](browser)
                                else:
                                    raise ValueError(f"Unsupported index type for {col_name}: {type(col_info['index'])}")
                                col_info['values'].append(scraped_value)
                        
                        # Append the same URL ID for every row from this URL only if auto_url_id is True
                        if auto_url_id:
                            if 'url_id' not in col_data:
                                col_data['url_id'] = {'index': None, 'values': []}
                            col_data['url_id']['values'].append(url_id_val)
                                
        except Exception as e:
            print(f'NO DATA FOUND ERROR: {e}')
    
    browser.close()
    
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

def aggregate_columns(df, columns: list = None, boolean_columns: list = None):
    """
    Universal aggregation function that returns mean, min, max, and std values for numeric columns
    and boolean aggregation for True/False columns
    
    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame
    columns : list
        List of numeric column names to aggregate. If None, aggregates all numeric columns.
    boolean_columns : list
        List of boolean column names to check for any True values.
    
    Returns
    -------
    pd.Series
        Aggregated statistics for the specified columns

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
    
    return pd.Series(agg)