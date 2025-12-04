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
import requests
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

def scrape_url_table(
    urls: list,
    min_col: int,
    max_col: int,
    col_idx_map: dict,
    id_cols: list = None,
    page_lvl_cols: list = None,
    data_folder: str = '../data/raw',
    id_mask: dict = None,
    auto_url_id: bool = False
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

    # Establish web browser
    browser = webdriver.Chrome()
    browser.maximize_window()
    
    for url in urls:
        
        # Validate URL
        try:
            browser.get(url)
            # Add a small wait to ensure page is loaded
            time.sleep(0.5)
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
                                
        except Exception as e:
            print(f'URL: {url}')
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
# V. Constructors Common Name Map
# ==============================================================================================

# Constructors common name mapping
constructor_mapping = {'team_id': {
    # Red Bull
    'Red Bull Racing Renault': 'Red Bull',
    'Red Bull Renault': 'Red Bull',
    'RBR Renault': 'Red Bull',
    'RBR Cosworth': 'Red Bull',
    'RBR Ferrari': 'Red Bull',
    'Red Bull Racing TAG Heuer': 'Red Bull',
    'Red Bull Racing Honda': 'Red Bull',
    'Red Bull Racing RBPT': 'Red Bull',
    'Red Bull Racing Honda RBPT': 'Red Bull',
    'Red Bull Racing': 'Red Bull',
    
    # AlphaTauri/Toro Rosso
    'Toro Rosso': 'Toro Rosso',
    'STR Ferrari': 'Toro Rosso',
    'STR Renault': 'Toro Rosso',
    'STR Cosworth': 'Toro Rosso',
    'Toro Rosso Ferrari': 'Toro Rosso',
    'Scuderia Toro Rosso Honda': 'Toro Rosso',
    'AlphaTauri Honda': 'AlphaTauri',
    'AlphaTauri RBPT': 'AlphaTauri',
    'AlphaTauri Honda RBPT': 'AlphaTauri',
    
    # Racing Bulls
    'RB Honda RBPT': 'Racing Bulls',
    
    # Ferrari
    'Ferrari': 'Ferrari',
    'Ferrari Jaguar': 'Ferrari',
    'Thin Wall Ferrari': 'Ferrari',
    
    # Mercedes
    'Mercedes': 'Mercedes',
    'Mercedes-Benz': 'Mercedes',
    
    # Aston Martin
    'Aston Martin Mercedes': 'Aston Martin',
    'Aston Martin Aramco Mercedes': 'Aston Martin',
    'Aston Butterworth': 'Aston Martin',
    'Aston Martin': 'Aston Martin',
    
    # McLaren
    'McLaren Ford': 'McLaren',
    'McLaren TAG': 'McLaren',
    'McLaren Honda': 'McLaren',
    'McLaren Peugeot': 'McLaren',
    'McLaren Renault': 'McLaren',
    'McLaren BRM': 'McLaren',
    'McLaren Mercedes': 'McLaren',
    'McLaren Serenissima': 'McLaren',
    'Mclaren BRM': 'McLaren',
    'McLaren Alfa Romeo': 'McLaren',
    
    # Williams
    'Williams Ford': 'Williams',
    'Williams Renault': 'Williams',
    'Williams Honda': 'Williams',
    'Williams Judd': 'Williams',
    'Williams BMW': 'Williams',
    'Williams Toyota': 'Williams',
    'Williams Cosworth': 'Williams',
    'Williams Mecachrome': 'Williams',
    'Williams Supertec': 'Williams',
    'Williams Mercedes': 'Williams',
    'Frank Williams Racing Cars/Williams': 'Williams',
    
    # Renault
    'Renault': 'Renault',

    # Alpine
    'Alpine Renault': 'Alpine',
    
    # Lotus
    'Lotus Renault': 'Lotus',
    'Lotus Ford': 'Lotus',
    'Lotus Climax': 'Lotus',
    'Lotus BRM': 'Lotus',
    'Lotus Honda': 'Lotus',
    'Lotus Judd': 'Lotus',
    'Lotus Lamborghini': 'Lotus',
    'Lotus Mugen Honda': 'Lotus',
    'Lotus Mercedes': 'Lotus',
    'Lotus Cosworth': 'Lotus',
    'Lotus Maserati': 'Lotus',
    'Lotus Pratt & Whitney': 'Lotus',
    
    # Force India
    'Force India Ferrari': 'Force India',
    'Force India Mercedes': 'Force India',

    # Racing Point
    'Racing Point BWT Mercedes': 'Racing Point',

    # Sauber
    'Sauber': 'Sauber',
    'Sauber Ferrari': 'Sauber',
    'Sauber Petronas': 'Sauber',
    'Sauber BMW': 'Sauber',
    'Sauber Mercedes': 'Sauber',
    'Sauber Ford': 'Sauber',
    'Kick Sauber Ferrari': 'Sauber',

    # Alfa Romeo
    'Alfa Romeo Racing Ferrari': 'Alfa Romeo',
    'Alfa Romeo Ferrari': 'Alfa Romeo',
    'Alfa Romeo': 'Alfa Romeo',
    
    # Haas
    'Haas Ferrari': 'Haas',
    'Haas F1 Team': 'Haas',
    
    # Jordan
    'Jordan Ford': 'Jordan',
    'Jordan Peugeot': 'Jordan',
    'Jordan Hart': 'Jordan',
    'Jordan Honda': 'Jordan',
    'Jordan Yamaha': 'Jordan',
    'Jordan Toyota': 'Jordan',
    'Jordan Mugen Honda': 'Jordan',
    
    # BAR
    'BAR Honda': 'BAR',
    'BAR Supertec': 'BAR',
    
    # Honda
    'Honda': 'Honda',
    
    # Benetton
    'Benetton Ford': 'Benetton',
    'Benetton BMW': 'Benetton',
    'Benetton Renault': 'Benetton',
    'Benetton Playlife': 'Benetton',
    
    # Toyota
    'Toyota': 'Toyota',
    
    # Jaguar
    'Jaguar Cosworth': 'Jaguar',
    
    # Stewart
    'Stewart Ford': 'Stewart',
    
    # BRM
    'BRM': 'BRM',
    'BRM Climax': 'BRM',

    # JBW
    'JBW Maserati': 'JBW',
    'JBW Climax': 'JBW',
    
    # Cooper
    'Cooper Climax': 'Cooper',
    'Cooper Maserati': 'Cooper',
    'Cooper Bristol': 'Cooper',
    'Cooper Castellotti': 'Cooper',
    'Cooper BRM': 'Cooper',
    'Cooper JAP': 'Cooper',
    'Cooper Alta': 'Cooper',
    'Cooper Borgward': 'Cooper',
    'Cooper Alfa Romeo': 'Cooper',
    'Cooper Ferrari': 'Cooper',
    'Cooper ATS': 'Cooper',
    'Cooper Ford': 'Cooper',
    'Cooper OSCA': 'Cooper',
    
    # Brabham
    'Brabham Climax': 'Brabham',
    'Brabham Repco': 'Brabham',
    'Brabham Ford': 'Brabham',
    'Brabham Alfa Romeo': 'Brabham',
    'Brabham BMW': 'Brabham',
    'Brabham BRM': 'Brabham',
    'Brabham Judd': 'Brabham',
    'Brabham Yamaha': 'Brabham',
    
    # Maserati
    'Maserati': 'Maserati',
    'Maserati Offenhauser': 'Maserati',
    'Maserati Milano': 'Maserati',
    'Maserati-Offenhauser': 'Maserati',
    'Maserati OSCA': 'Maserati',
    'Maserati Plate': 'Maserati',
    
    # Ligier
    'Ligier Matra': 'Ligier',
    'Ligier Ford': 'Ligier',
    'Ligier Renault': 'Ligier',
    'Ligier Megatron': 'Ligier',
    'Ligier Mugen Honda': 'Ligier',
    
    # Tyrrell
    'Tyrrell Ford': 'Tyrrell',
    'Tyrrell Renault': 'Tyrrell',
    'Tyrrell Honda': 'Tyrrell',
    'Tyrrell Yamaha': 'Tyrrell',
    'Tyrrell Ilmor': 'Tyrrell',
    
    # Arrows/Footwork
    'Arrows Ford': 'Arrows',
    'Arrows BMW': 'Arrows',
    'Arrows Megatron': 'Arrows',
    'Arrows Yamaha': 'Arrows',
    'Arrows Supertec': 'Arrows',
    'Arrows Asiatech': 'Arrows',
    'Arrows Cosworth': 'Arrows',
    'Arrows': 'Arrows',
    'Footwork Ford': 'Footwork',
    'Footwork Hart': 'Footwork',
    'Footwork Mugen Honda': 'Footwork',
    'Footwork Porsche': 'Footwork',
    
    # Vanwall
    'Vanwall': 'Vanwall',
    
    # Wolf
    'Wolf Ford': 'Wolf',
    'Wolf-Williams': 'Wolf',
    
    # Lola
    'Lola Ford': 'Lola',
    'Lola Lamborghini': 'Lola',
    'Lola Climax': 'Lola',
    'Lola BMW': 'Lola',
    'Lola Hart': 'Lola',
    'Lola Ferrari': 'Lola',

    # March
    'March Ford': 'March',
    'March Judd': 'March',
    'March Ilmor': 'March',
    'March Alfa Romeo': 'March',

    # Minardi
    'Minardi Ford': 'Minardi',
    'Minardi Ferrari': 'Minardi',
    'Minardi Lamborghini': 'Minardi',
    'Minardi Asiatech': 'Minardi',
    'Minardi Cosworth': 'Minardi',
    'Minardi Fondmetal': 'Minardi',
    'Minardi European': 'Minardi',
    'Minardi Hart': 'Minardi',
    'Minardi Motori Moderni': 'Minardi',
    
    # LDS
    'LDS Alfa Romeo': 'LDS',
    'LDS Climax': 'LDS',
    'LDS Repco': 'LDS',

    # Porche
    'Porsche (F2)': 'Porsche',
    'Porsche': 'Porsche',
    'Behra-Porsche': 'Porsche',

    # Scirocco
    'Scirocco BRM': 'Scirocco',
    'Scirocco Climax': 'Scirocco',

    # AFM
    'AFM Kuchen': 'AFM',
    'AFM BMW': 'AFM',
    'AFM Bristol': 'AFM',

    # ATS
    'ATS Ford': 'ATS',
    'ATS': 'ATS',
    'ATS BMW': 'ATS',
    'Derrington-Francis ATS': 'ATS',

    # Leyton House
    'Leyton House Judd': 'Leyton House',
    'Leyton House Ilmor': 'Leyton House',

    # Prost
    'Prost Mugen Honda': 'Prost',
    'Prost Peugeot': 'Prost',
    'Prost Acer': 'Prost',

    # Dallara
    'Dallara Judd': 'Dallara',
    'Dallara Ferrari': 'Dallara',
    'Dallara Ford': 'Dallara',

    # Larrousse
    'Larrousse Lamborghini': 'Larrousse',
    'Larrousse Ford': 'Larrousse',

    # Osella
    'Osella Ford': 'Osella',
    'Osella Alfa Romeo': 'Osella',
    'Osella': 'Osella',
    'Osella Hart': 'Osella',

    # Kurtis Kraft
    'Kurtis Kraft Offenhauser': 'Kurtis Kraft',
    'Kurtis Kraft Novi': 'Kurtis Kraft',
    'Kurtis Kraft Cummins': 'Kurtis Kraft',

    # Marussia
    'Marussia Cosworth': 'Marussia',
    'Marussia Ferrari': 'Marussia',

    # Gordini
    'Simca-Gordini': 'Gordini',
    'Gordini': 'Gordini',

    # Connaught
    'Connaught Lea Francis': 'Connaught',
    'Connaught Alta': 'Connaught',

    # Eagle
    'Eagle Climax': 'Eagle',
    'Eagle Weslake': 'Eagle',

    # RAM
    'RAM Ford': 'RAM',
    'RAM Hart': 'RAM',

    # Shadow
    'Shadow Ford': 'Shadow',
    'Shadow Matra': 'Shadow',

    # Matra
    'Matra Ford': 'Matra',
    'Matra': 'Matra',
    'Matra Cosworth': 'Matra',
    'Matra BRM': 'Matra',

    # ERA
    'ERA': 'ERA',
    'ERA Bristol': 'ERA',

    # Spirit
    'Spirit Honda': 'Spirit',   
    'Spirit Hart': 'Spirit',

    # Frazer Nash
    'Frazer Nash': 'Frazer Nash',
    'Frazer Nash Bristol': 'Frazer Nash',

    # Emeryson
    'Emeryson Alta': 'Emeryson',
    'Emeryson Climax': 'Emeryson',

    # De Tomaso
    'De Tomaso OSCA': 'De Tomaso',
    'De Tomaso Alfa Romeo': 'De Tomaso',
    'De Tomaso Ford': 'De Tomaso',

    # Gilby
    'Gilby Climax': 'Gilby',
    'Gilby BRM': 'Gilby',

    # Tecno
    'Tecno': 'Tecno',
    'Tecno Cosworth': 'Tecno',

    # Ligier
    'Ligier Judd': 'Ligier',
    'Ligier Lamborghini': 'Ligier',

    # Euro Brun
    'Euro Brun Judd': 'Euro Brun',
    'Euro Brun Ford': 'Euro Brun',


    # Other
    'No Team': 'Privateer',
    'Toleman Hart': 'Toleman',       
    'Venturi Lamborghini': 'Venturi',        
    'Onyx Ford': 'Onyx',
    'AGS Ford': 'AGS',   
    'Rial Ford': 'Rial',
    'Zakspeed': 'Zakspeed',
    'Theodore Ford': 'Theodore',
    'Deidt Offenhauser': 'Deidt',
    'Sherman Offenhauser': 'Sherman',
    'Schroeder Offenhauser': 'Schroeder',
    'Kuzma Offenhauser': 'Kuzma',
    'Lesovsky Offenhauser': 'Lesovsky',
    'Watson Offenhauser': 'Watson',
    'Phillips Offenhauser': 'Phillips',
    'Epperly Offenhauser': 'Epperly',
    'Trevis Offenhauser': 'Trevis',
    'HRT Cosworth': 'HRT',
    'Virgin Cosworth': 'Virgin',
    'Caterham Renault': 'Caterham',
    'Milano Speluzzi': 'Milano',
    'Turner Offenhauser': 'Turner',
    'Alta': 'Alta',    
    'Moore Offenhauser': 'Moore',
    'Nichels Offenhauser': 'Nichels',
    'Marchese Offenhauser': 'Marchese',
    'Stevens Offenhauser': 'Stevens',
    'Langley Offenhauser': 'Langley',
    'Ewing Offenhauser': 'Ewing',   
    'Rae Offenhauser': 'Rae',
    'Olson Offenhauser': 'Olson',
    'Wetteroth Offerhauser': 'Wetteroth',
    'Snowberger Offenhauser': 'Snowberger',
    'Adams Offenhauser': 'Adams',
    'HWM Alta': 'HWM',    
    'Lancia': 'Lancia',
    'Talbot-Lago': 'Talbot-Lago',
    'BRP BRM': 'BRP',
    'Hesketh Ford': 'Hesketh',
    'Hill Ford': 'Hill',
    'Ensign Ford': 'Ensign',
    'Penske Ford': 'Penske',
    'Fittipaldi Ford': 'Fittipaldi',
    'ISO Marlboro Ford': 'ISO Marlboro',
    'Iso Marlboro Ford': 'ISO Marlboro',
    'Surtees Ford': 'Surtees',
    'Parnelli Ford': 'Parnelli',
    'Super Aguri Honda': 'Super Aguri',
    'MRT Mercedes': 'Manor',
    'Brawn Mercedes': 'Brawn',
    'Spyker Ferrari': 'Spyker',
    'MF1 Toyota': 'Midland',
    'Veritas': 'Veritas',
    'Pawl Offenhauser': 'Pawl',
    'Hall Offenhauser': 'Hall',
    'Bromme Offenhauser': 'Bromme',
    'OSCA': 'OSCA',
    'BMW': 'BMW',
    'EMW': 'EMW',
    'Pankratz Offenhauser': 'Pankratz',
    'Bugatti': 'Bugatti',
    'Klenk BMW': 'Klenk',
    'Dunn Offenhauser': 'Dunn',    
    'Elder Offenhauser': 'Elder',
    'Christensen Offenhauser': 'Christensen',
    'Sutton Offenhauser': 'Sutton',
    'Tec-Mec Maserati': 'Tec-Mec',
    'Meskowski Offenhauser': 'Meskowski',
    'Scarab': 'Scarab',
    'Ferguson Climax': 'Ferguson',
    'ENB Maserati': 'ENB',
    'Stebro Ford': 'Stebro',               
    'Shannon Climax': 'Shannon',     
    'Protos Cosworth': 'Protos',   
    'Bellasi Ford': 'Bellasi',       
    'Eifelland Ford': 'Eifelland',
    'Politoys Ford': 'Politoys',
    'Connew Ford': 'Connew',
    'Trojan Ford': 'Trojan',
    'Amon Ford': 'Amon',
    'Token Ford': 'Token',
    'Lyncar Ford': 'Lyncar',
    'Boro Ford': 'Boro',
    'Kojima Ford': 'Kojima',
    'LEC Ford': 'LEC',
    'Merzario Ford': 'Merzario',
    'Martini Ford': 'Martini',
    'Rebaque Ford': 'Rebaque',
    'AGS Motori Moderni': 'AGS',
    'Coloni Ford': 'Coloni',
    'Zakspeed Yamaha': 'Zakspeed',
    'Fondmetal Ford': 'Fondmetal',
    'Moda Judd': 'Moda',    
    'Simtek Ford': 'Simtek',
    'Pacific Ilmor': 'Pacific',
    'Forti Ford': 'Forti',
    'Lambo Lamborghini': 'Modena'
}}


# ==============================================================================================
# VI. Constructors Common Name Map
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
        Returns None if location is not found or an error occurs
    """
    try:
        # Combine inputs into a single query string
        query = f"{place}, {city}, {country}"
        
        # Get coordinates from Photon API
        url = f"https://photon.komoot.io/api/?q={query}"
        response = requests.get(url).json()
        
        if not response['features']:
            print("No results found")
            return None
        
        coords = response['features'][0]['geometry']['coordinates']
        lat, lon = coords[1], coords[0]
        
        # Get elevation from Open Elevation API
        elevation_url = f"https://api.open-elevation.com/api/v1/lookup?locations={lat},{lon}"
        elevation_response = requests.get(elevation_url).json()
        elevation = elevation_response['results'][0]['elevation']
        
        return {
            'latitude': lat,
            'longitude': lon,
            'elevation': elevation
        }
    
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None
    except (KeyError, IndexError) as e:
        print(f"Error parsing response: {e}")
        return None