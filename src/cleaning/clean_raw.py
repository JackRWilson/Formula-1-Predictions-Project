# Jack Wilson
# 1/19/2026
# This file cleans all raw data in preparation for merging

# --------------------------------------------------------------------------------
# Import modules

import pandas as pd
import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.utils.utils import load_id_map, save_id_map
from src.utils.project_functions import convert_position, constructor_mapping

DATA_FOLDER_PATH = os.path.join(PROJECT_ROOT, 'data/raw')
CLEAN_FOLDER_PATH = os.path.join(PROJECT_ROOT, 'data/clean')


# --------------------------------------------------------------------------------
# ID Map

def clean_id_map():
    
    print("   Cleaning Circuit ID Map...")

    # Init variables
    raw_file_name = 'circuit_id_map.pkl'
    load_path = os.path.join(DATA_FOLDER_PATH, raw_file_name)
    save_path = os.path.join(DATA_FOLDER_PATH, raw_file_name)

    # Load file
    id_map = load_id_map(load_path)
    
    # Ensure Austria and Styria have the same circuit ID
    if 'Styria' in id_map and 'Austria' in id_map:
        id_map['Styria'] = id_map['Austria']

    # Ensure Great Britain and 70th Anniversary have the same circuit ID
    if '70th Anniversary' in id_map and 'Great Britain' in id_map:
        id_map['70th Anniversary'] = id_map['Great Britain']
    
    # Save file
    save_id_map(save_path, id_map)
    print("   Circuit ID Map cleaned")


# --------------------------------------------------------------------------------
# Race Results 2001-2017

def clean_results_2001():

    print("   Cleaning Results 2001-2017...")

    # Init variables
    raw_file_name = 'race_results_raw_2001-2017.csv'
    clean_file_name = 'race_results_clean_2001-2017.csv'
    load_path = os.path.join(DATA_FOLDER_PATH, raw_file_name)
    save_path = os.path.join(CLEAN_FOLDER_PATH, clean_file_name)
    
    # Load file
    races_2001 = pd.read_csv(load_path)

    # Convert date column to datetime
    for i, date in enumerate(races_2001['date']):
        if '-' in date:
            races_2001.at[i, 'date'] = date.split('-')[1].strip()
    races_2001['date'] = pd.to_datetime(races_2001['date'], format='mixed')
    
    # Create year column
    races_2001['year'] = races_2001['date'].dt.year

    # Create round column
    races_2001 = races_2001.sort_values(['year', 'date'])
    races_2001['round'] = races_2001.groupby('year')['date'].transform(lambda x: x.rank(method='dense').astype(int))

    # Separate position status
    races_2001['position_status'] = 'CLAS'
    for idx, row in races_2001.iterrows():
        try:
            int(row['position'])
        except (ValueError, TypeError):
            races_2001.at[idx, 'position_status'] = row['position']
    races_2001['position_status'] = races_2001['position_status'].replace('EX', 'DQ')

    # Convert position to numeric
    prev_pos = None
    converted_positions = []
    for idx, row in races_2001.iterrows():
        current_pos = convert_position(row, prev_pos)
        converted_positions.append(current_pos)
        prev_pos = current_pos
    races_2001['position'] = converted_positions

    # Save file
    races_2001.to_csv(save_path, encoding='utf-8', index=False)
    print("   Results 2001-2017 cleaned")