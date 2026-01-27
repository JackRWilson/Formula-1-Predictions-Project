# Jack Wilson
# 1/26/2026
# This file merges all cleaned data into pre-qualifying and pre-race dataframes

# --------------------------------------------------------------------------------
# Import modules

import pandas as pd
import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
#from src.utils.utils import
#from src.utils.project_functions import

DATA_FOLDER_PATH = os.path.join(PROJECT_ROOT, 'data/raw')
CLEAN_FOLDER_PATH = os.path.join(PROJECT_ROOT, 'data/clean')
INTERMEDIATE_FOLDER_PATH = os.path.join(PROJECT_ROOT, 'data/intermediate')


# --------------------------------------------------------------------------------
# Neutral merges

def neutral_merge():
    print("   Creating neutral DataFrame...")
    
    # Load data
    races_2018 = pd.read_csv(os.path.join(CLEAN_FOLDER_PATH, 'race_results_clean_2018+.csv'))
    races_2001 = pd.read_csv(os.path.join(CLEAN_FOLDER_PATH, 'race_results_clean_2001-2017.csv'))
    practices = pd.read_csv(os.path.join(CLEAN_FOLDER_PATH, 'practice_results_clean.csv'))
    pit_stops = pd.read_csv(os.path.join(CLEAN_FOLDER_PATH, 'pit_stops_clean.csv'))
    flags = pd.read_csv(os.path.join(CLEAN_FOLDER_PATH, 'flags_clean.csv'))
    circuits = pd.read_csv(os.path.join(CLEAN_FOLDER_PATH, 'circuits_clean.csv'))
    locations = pd.read_csv(os.path.join(CLEAN_FOLDER_PATH, 'locations_clean.csv'))
    rounds = pd.read_csv(os.path.join(DATA_FOLDER_PATH, 'rounds_raw.csv'))

    # Make merges
    f1_data_m1 = races_2018.merge(rounds, on='race_url', how='left')
    f1_data_m2 = f1_data_m1.merge(practices, on=['race_id', 'driver_id'], how='left')
    f1_data_m3 = f1_data_m2.merge(pit_stops, on=['race_id', 'driver_id'], how='left')
    f1_data_m4 = f1_data_m3.merge(flags, on=['race_id', 'circuit_id'], how='left')
    f1_data_m5 = f1_data_m4.merge(circuits, on='circuit_id', how='left')
    f1_data_m6 = f1_data_m5.merge(locations, on='name', how='left')

    # Save merged data
    f1_data_m6.to_csv(os.path.join(INTERMEDIATE_FOLDER_PATH, 'f1_data_neutral_merges.csv'), encoding='utf-8', index=False)
    print("   Neutral DataFrame created")