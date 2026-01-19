# Jack Wilson
# 1/19/2026
# This file cleans all raw data in preparation for merging

# --------------------------------------------------------------------------------
# Import modules

import pandas as pd
import os, sys, re
from datetime import timedelta

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
    global id_map_styria
    if 'Styria' in id_map and 'Austria' in id_map:
        id_map_styria = id_map['Styria']
        id_map['Styria'] = id_map['Austria']

    # Ensure Great Britain and 70th Anniversary have the same circuit ID
    global id_map_anniversary
    if '70th Anniversary' in id_map and 'Great Britain' in id_map:
        id_map_anniversary = id_map['70th Anniversary']
        id_map['70th Anniversary'] = id_map['Great Britain']
    
    # Save file
    save_id_map(save_path, id_map)
    print("   Circuit ID Map cleaned")


# --------------------------------------------------------------------------------
# Race Results 2001-2017

def clean_results_2001():

    print("   Cleaning Results (2001-2017)...")

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
    print("   Results (2001-2017) cleaned")


# --------------------------------------------------------------------------------
# Race Results 2018+

def clean_results_2018():

    print("   Cleaning Results (2018+)...")

    # Init variables
    raw_file_name = 'race_results_raw_2018+.csv'
    clean_file_name = 'race_results_clean_2018+.csv'
    load_path = os.path.join(DATA_FOLDER_PATH, raw_file_name)
    save_path = os.path.join(CLEAN_FOLDER_PATH, clean_file_name)
    
    # Load file
    races_2018 = pd.read_csv(load_path)

    # Fix circuit id
    races_2018['circuit_id'] = races_2018['circuit_id'].replace({id_map_styria: 10, id_map_anniversary: 9})

    # Initialize new columns
    end_positions = []
    statuses = []
    current_position = 1
    current_race = None

    # Separate position status
    for idx, row in races_2018.iterrows():
        if current_race != row['race_url']:
            current_race = row['race_url']
            current_position = 1
        pos = row['end_position']
        try:
            # Try converting to int to validate position
            numeric_pos = int(pos)
            end_positions.append(numeric_pos)
            statuses.append('CLAS')
            current_position = numeric_pos + 1
        except ValueError:
            # Not a number so need to assign position and keep status
            if pos in ['NC', 'DQ']:
                end_positions.append(current_position)
                statuses.append(pos)
                current_position += 1
            else:
                end_positions.append(current_position)
                statuses.append('DNF')
                current_position += 1
    races_2018['end_position'] = end_positions
    races_2018['position_status'] = statuses

    # Impute 0 for laps_completed if null
    races_2018['laps_completed'] = races_2018['laps_completed'].fillna(0)

    # Map team names to constructor common names using existing constructor_mapping
    races_2018['team_name'] = races_2018['team_name'].map(constructor_mapping['team_id']).fillna(races_2018['team_name'])

    # Save file
    races_2018.to_csv(save_path, encoding='utf-8', index=False)
    print("   Results (2018+) cleaned")


# --------------------------------------------------------------------------------
# Practices

def clean_practices_2018():

    print("   Cleaning Practices (2018+)...")

    # Init variables
    raw_file_name = 'pratice_results_raw.csv'
    clean_file_name = 'practice_results_clean.csv'
    load_path = os.path.join(DATA_FOLDER_PATH, raw_file_name)
    save_path = os.path.join(CLEAN_FOLDER_PATH, clean_file_name)
    
    # Load file
    practices = pd.read_csv(load_path)

    # Add recorded lap column
    practices['recorded_lap_time'] = practices['lap_time'].notna()

    # Convert lap times
    current_race_id = None
    current_session_type = None
    base_time = None

    for idx, row in practices.iterrows():
        lap_time = row['lap_time']
        
        # Check if starting a new race_id and session_type group
        if current_race_id != row['race_id'] or current_session_type != row['session_type']:
            current_race_id = row['race_id']
            current_session_type = row['session_type']
            base_time = None
        
        try:
            if pd.notna(lap_time):
                # Check if this is a base time
                if not lap_time.startswith('+'):
                    if ':' in lap_time:
                        # Time in "min:sec.millisec" format
                        time_parts = re.split(r"[:.]", lap_time)
                        minutes = int(time_parts[0])
                        seconds = int(time_parts[1])
                        milliseconds = int(time_parts[2])
                    else:
                        # Time in "sec.millisec" format
                        time_parts = lap_time.split('.')
                        minutes = 0
                        seconds = int(time_parts[0])
                        milliseconds = int(time_parts[1])
                    
                    # Convert to timedelta and store as base time
                    base_time = timedelta(minutes=minutes, seconds=seconds, milliseconds=milliseconds)
                    practices.at[idx, 'lap_time_clean'] = base_time
                else:
                    if base_time is not None:
                        # Get rid of the + and s
                        time_clean = lap_time.strip('+s')
                        
                        # Parse the gap time
                        if ':' in time_clean:
                            # Gap time in "min:sec.millisec" format
                            time_parts = re.split(r"[:.]", time_clean)
                            gap_minutes = int(time_parts[0])
                            gap_seconds = int(time_parts[1])
                            gap_milliseconds = int(time_parts[2])
                        else:
                            # Gap time in "sec.millisec" format
                            time_parts = time_clean.split('.')
                            gap_minutes = 0
                            gap_seconds = int(time_parts[0])
                            gap_milliseconds = int(time_parts[1])
                        
                        # Convert gap to timedelta and add to base time
                        gap = timedelta(minutes=gap_minutes, seconds=gap_seconds, milliseconds=gap_milliseconds)
                        new_time = base_time + gap
                        practices.at[idx, 'lap_time_clean'] = new_time
        
        except (ValueError, AttributeError):
            if pd.isna(lap_time):
                practices.at[idx, 'lap_time_clean'] = None
                continue
            else:
                # Handle unexpected format
                practices.at[idx, 'lap_time_clean'] = None
    
    # Impute missing lap times
    for (race_id, session_type), group in practices.groupby(['race_id', 'session_type']):
        if group['lap_time_clean'].isna().any():
            
            # Get the most recent non-null value in this group if any
            most_recent_time = group['lap_time_clean'].dropna().iloc[-1] if not group['lap_time_clean'].dropna().empty else None
            
            if most_recent_time is not None:
                imputed_time = most_recent_time * 1.05  # 1.05x time multiplier
                
                # Impute the time to missing values in this group
                missing_indices = group[group['lap_time_clean'].isna()].index
                practices.loc[missing_indices, 'lap_time_clean'] = imputed_time
    
    # Drop unnecessary columns
    practices.drop(['lap_time', 'team_id'], axis=1, inplace=True)

    # Format session type
    session_map = {
        'practice0': 'FP3',
        'practice1': 'FP1',
        'practice2': 'FP2',
        'practice3': 'FP3'
    }
    practices['session_type'] = practices['session_type'].map(session_map)

    # Correct datatypes
    practices['best_time'] = practices['lap_time_clean'].dt.total_seconds()
    practices.drop('lap_time_clean', axis=1, inplace=True)

    # Convert long to wide
    practices_pivot = practices.pivot_table(
        index=['race_id', 'driver_id'],
        columns='session_type',
        values=['best_time', 'lap_count', 'position', 'recorded_lap_time'],
        aggfunc='first'
    )
    practices_pivot.columns = [f'{col[0]}_{col[1]}' for col in practices_pivot.columns]
    practices_aggregated = practices_pivot.reset_index()

    # Add participation column
    for session in ['FP1', 'FP2', 'FP3']:
        best_time_col = f'best_time_{session}'
        participated_col = f'participated_{session}'
        
        practices_aggregated[participated_col] = practices_aggregated[best_time_col].notna()

    # Fill NA values in best_time columns with 0
    best_time_cols = [col for col in practices_aggregated.columns if col.startswith('best_time_')]
    practices_aggregated[best_time_cols] = practices_aggregated[best_time_cols].fillna(0)

    # Fill NA values in recorded_lap_time columns with False
    recorded_cols = [col for col in practices_aggregated.columns if col.startswith('recorded_lap_time_')]
    practices_aggregated[recorded_cols] = practices_aggregated[recorded_cols].fillna(False)

    # Fill NA values in lap_count columns with 0
    lap_count_cols = [col for col in practices_aggregated.columns if col.startswith('lap_count_')]
    practices_aggregated[lap_count_cols] = practices_aggregated[lap_count_cols].fillna(0)

    # Impute position
    for session in ['FP1', 'FP2', 'FP3']:
        position_col = f'position_{session}'
        for race_id, group in practices_aggregated.groupby('race_id'):
            race_indices = group.index
            
            # Find missing positions in this race group
            missing_mask = practices_aggregated.loc[race_indices, position_col].isna()
            if missing_mask.any():
                
                # Get max position value in this race group
                max_position = practices_aggregated.loc[race_indices, position_col].max()
                
                # If all positions are NA, start from 1
                if pd.isna(max_position):
                    max_position = 0
                
                # Count how many missing positions we have
                num_missing = missing_mask.sum()
                
                # Assign imputed values
                imputed_values = [max_position + i + 1 for i in range(num_missing)]
                practices_aggregated.loc[race_indices[missing_mask], position_col] = imputed_values

    # Fix datatypes
    for session in ['FP1', 'FP2', 'FP3']:
        practices_aggregated[f'position_{session}'] = practices_aggregated[f'position_{session}'].astype(int)
        practices_aggregated[f'lap_count_{session}'] = practices_aggregated[f'lap_count_{session}'].astype(int)

    # Save file
    practices_aggregated.to_csv(save_path, encoding='utf-8', index=False)
    print("   Practices (2018+) cleaned")