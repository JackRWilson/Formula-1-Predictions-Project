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
from src.utils.project_functions import constructor_mapping

DATA_FOLDER_PATH = os.path.join(PROJECT_ROOT, 'data/raw')
CLEAN_FOLDER_PATH = os.path.join(PROJECT_ROOT, 'data/clean')


# --------------------------------------------------------------------------------
# ID Map

def clean_id_map():
    
    # Load file
    file_name = 'circuit_id_map.pkl'
    id_map = load_id_map(os.path.join(DATA_FOLDER_PATH, file_name))
    
    # Ensure Austria and Styria have the same circuit ID
    if 'Styria' in id_map and 'Austria' in id_map:
        id_map_styria = id_map['Styria']
        if id_map['Styria'] != id_map['Austria']:
            print(f"   Styria Circuit ID: {id_map_styria} -> {id_map['Austria']}")
            print(f"   Austria Circuit ID: {id_map['Austria']}")
        id_map['Styria'] = id_map['Austria']

    # Ensure Great Britain and 70th Anniversary have the same circuit ID
    if '70th Anniversary' in id_map and 'Great Britain' in id_map:
        id_map_anniversary = id_map['70th Anniversary']
        if id_map['70th Anniversary'] != id_map['Great Britain']:
            print(f"   70th Anniversary Circuit ID: {id_map_anniversary} -> {id_map['Great Britain']}")
            print(f"   Great Britain Circuit ID: {id_map['Great Britain']}")
        id_map['70th Anniversary'] = id_map['Great Britain']
    
    # Save file
    save_id_map(os.path.join(DATA_FOLDER_PATH, file_name), id_map)