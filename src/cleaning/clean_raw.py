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
from src.utils.project_functions import convert_position, constructor_mapping, clean_qualifying_times, convert_pit_time, impute_pit_times

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


# --------------------------------------------------------------------------------
# Qualifying

def clean_qualifying_2018():

    print("   Cleaning Qualifying (2018+)...")

    # Init variables
    raw_file_name = 'qualifying_results_raw.csv'
    clean_file_name = 'qualifying_results_clean.csv'
    load_path = os.path.join(DATA_FOLDER_PATH, raw_file_name)
    save_path = os.path.join(CLEAN_FOLDER_PATH, clean_file_name)
    
    # Load file
    qualifying = pd.read_csv(load_path)

    # Drop excess columns
    qualifying.drop(['team_id', 'qual_laps'], axis=1, inplace=True)

    # Convert non-numeric places
    qual_positions = []
    current_position = 1
    current_race = None

    # Group by each race
    for idx, row in qualifying.iterrows():
        if current_race != row['race_id']:
            current_race = row['race_id']
            current_position = 1
        pos = row['qual_position']
        try:
            # Try converting to int to validate position
            numeric_pos = int(pos)
            qual_positions.append(numeric_pos)
            current_position = numeric_pos + 1
        except ValueError:
            # Not a number so assign next available position
            qual_positions.append(current_position)
            current_position += 1
    qualifying['qual_position'] = qual_positions

    # Clean and impute lap times
    qualifying_cleaned = clean_qualifying_times(qualifying)

    # Correct datatypes
    qualifying_cleaned['q1_no_lap_time_flag'] = qualifying_cleaned['q1_no_lap_time_flag'].astype(bool)
    qualifying_cleaned['q2_no_lap_time_flag'] = qualifying_cleaned['q2_no_lap_time_flag'].astype(bool)
    qualifying_cleaned['q3_no_lap_time_flag'] = qualifying_cleaned['q3_no_lap_time_flag'].astype(bool)

    # Save file
    qualifying_cleaned.to_csv(save_path, encoding='utf-8', index=False)
    print("   Qualifying (2018+) cleaned")


# --------------------------------------------------------------------------------
# Starting Grid

def clean_starting_grid_2018():

    print("   Cleaning Starting Grid (2018+)...")

    # Init variables
    raw_file_name = 'starting_grid_results_raw.csv'
    clean_file_name = 'starting_grid_clean.csv'
    load_path = os.path.join(DATA_FOLDER_PATH, raw_file_name)
    save_path = os.path.join(CLEAN_FOLDER_PATH, clean_file_name)
    
    # Load file
    starting = pd.read_csv(load_path)

    # Drop excess columns
    starting.drop('team_id', axis=1, inplace=True)

    # Save file
    starting.to_csv(save_path, encoding='utf-8', index=False)
    print("   Starting Grid (2018+) cleaned")


# --------------------------------------------------------------------------------
# Pit Stops

def clean_pit_stops_2018():

    print("   Cleaning Pit Stops (2018+)...")

    # Init variables
    raw_file_name = 'pit_stop_results_raw.csv'
    clean_file_name = 'pit_stops_clean.csv'
    load_path = os.path.join(DATA_FOLDER_PATH, raw_file_name)
    save_path = os.path.join(CLEAN_FOLDER_PATH, clean_file_name)
    
    # Load files
    pit_stops = pd.read_csv(load_path)
    pit_stops_2016 = pd.read_csv(os.path.join(DATA_FOLDER_PATH, 'pit_stop_results_raw_2016-2017.csv'))

    # Convert date
    for i, date in enumerate(pit_stops_2016['date']):
        if '-' in date:
            pit_stops_2016.at[i, 'date'] = date.split('-')[1].strip()
    pit_stops_2016['date'] = pd.to_datetime(pit_stops_2016['date'], format='mixed')

    # Create negative race_id for sorting
    pit_stops_2016['race_id'] = -1 * (pit_stops_2016['date'].rank(method='dense', ascending=True).astype(int))
    pit_stops_2016.drop('date', axis=1, inplace=True)

    # Merge all pit years
    pit_stops = pd.concat([pit_stops, pit_stops_2016], axis=0, ignore_index=True)

    # Convert pit time from min:sec.millisec format to total seconds
    pit_stops['pits_time_seconds'] = pit_stops['pits_time'].apply(convert_pit_time)

    # Remove outliers
    pit_stops_bound = pit_stops[pit_stops['pits_time_seconds'] <= 120]  # Over 120 seconds

    # Aggregate stops
    pit_stops_sorted = pit_stops_bound.sort_values(['driver_id', 'race_id', 'stop_number'])
    pit_stops_sorted['pit_count'] = pit_stops_sorted.groupby('driver_id').cumcount() + 1

    # Calculate rolling averages for last 5 and last 10 pits (excluding current race)
    pit_stops_sorted['avg_pit_time_last_5'] = pit_stops_sorted.groupby('driver_id')['pits_time_seconds']\
        .transform(lambda x: x.shift(1).rolling(window=5, min_periods=1).mean())
    pit_stops_sorted['avg_pit_time_last_10'] = pit_stops_sorted.groupby('driver_id')['pits_time_seconds']\
        .transform(lambda x: x.shift(1).rolling(window=10, min_periods=1).mean())

    # Aggregate to get max stop number and sum of pit times for current race
    pit_stops_clean = pit_stops_sorted.groupby(['race_id', 'driver_id', 'team_id']).agg({
        'avg_pit_time_last_5': 'last',
        'avg_pit_time_last_10': 'last'
    }).reset_index()

    # Impute missing times
    pit_stops_bound = pit_stops_bound.sort_values(['race_id', 'stop_number']).reset_index(drop=True)
    pit_stops_clean['avg_pit_time_last_5'] = pit_stops_clean['avg_pit_time_last_5'].fillna(
        pit_stops_clean.apply(lambda row: impute_pit_times(row, pit_stops_bound) if pd.isna(row['avg_pit_time_last_5']) else row['avg_pit_time_last_5'], axis=1)
    )
    pit_stops_clean['avg_pit_time_last_10'] = pit_stops_clean['avg_pit_time_last_10'].fillna(
        pit_stops_clean.apply(lambda row: impute_pit_times(row, pit_stops_bound) if pd.isna(row['avg_pit_time_last_10']) else row['avg_pit_time_last_10'], axis=1)
    )

    # Drop excess column and rows
    pit_stops_clean.drop('team_id', axis=1, inplace=True)
    pit_stops_clean = pit_stops_clean[pit_stops_clean['race_id'] >= 0]

    # Save file
    pit_stops_clean.to_csv(save_path, encoding='utf-8', index=False)
    print("   Pit Stops (2018+) cleaned")


# --------------------------------------------------------------------------------
# Laps

def clean_laps():

    print("   Cleaning Laps...")

    # Init variables
    raw_file_name = 'lap_results_raw.csv'
    clean_file_name = 'laps_clean.csv'
    load_path = os.path.join(DATA_FOLDER_PATH, raw_file_name)
    save_path = os.path.join(CLEAN_FOLDER_PATH, clean_file_name)
    
    # Load file
    laps = pd.read_csv(load_path)

    # Fill unknown race ID (70th anniversary)
    results = pd.read_csv(os.path.join(CLEAN_FOLDER_PATH, 'race_results_clean_2018+.csv'))
    race_id_70th = results.loc[results['circuit_name'] == '70th Anniversary', 'race_id'].iloc[0]
    laps['race_id'] = laps['race_id'].replace('unknown', race_id_70th)

    # Drop missing drivers
    laps = laps.dropna(subset=['driver_id'])

    # Drop excess columns
    laps.drop(['driver_name', 'laps_on_soft', 'laps_on_medium', 'laps_on_hard', 'laps_on_intermediate', 'laps_on_wet'], axis=1, inplace=True)
    laps_filtered = laps[laps['session'].isin(['FP1', 'FP2', 'FP3', 'Qualifying'])]

    # Add boolean flags
    compounds = ['soft', 'medium', 'hard', 'intermediate', 'wet']
    for c in compounds:
        flag_col = f'used_{c}'
        laps_filtered[flag_col] = (
            laps_filtered[f'avg_pace_{c}'].notna() |
            laps_filtered[f'deg_rate_{c}'].notna() |
            laps_filtered[f'std_pace_{c}'].notna()
            )

    # Convert long to wide
    laps_pivot = laps_filtered.pivot_table(
    index=['race_id', 'driver_id'],
    columns='session',
    values=['avg_pace_soft', 'std_pace_soft', 'deg_rate_soft',
    'avg_pace_medium', 'std_pace_medium', 'deg_rate_medium',
    'avg_pace_hard', 'std_pace_hard', 'deg_rate_hard',
    'avg_pace_intermediate', 'std_pace_intermediate', 'deg_rate_intermediate',
    'avg_pace_wet', 'std_pace_wet', 'deg_rate_wet',
    'used_soft', 'used_medium', 'used_hard', 'used_intermediate', 'used_wet'],
    aggfunc='first'
    )
    laps_pivot.columns = [f'{col[0]}_{col[1]}' for col in laps_pivot.columns]
    laps_aggregated = laps_pivot.reset_index()

    # Fill binary used columns with False indicating compound wasn't used
    used_cols = [col for col in laps_aggregated.columns if col.startswith('used_')]
    laps_aggregated[used_cols] = laps_aggregated[used_cols].fillna(False)

    # Fill missing data
    sessions = ['FP1', 'FP2', 'FP3', 'Qualifying']
    for s in sessions:
        for c in compounds:
            laps_aggregated[f'avg_pace_{c}_{s}'] = laps_aggregated[f'avg_pace_{c}_{s}'].fillna(0)
            laps_aggregated[f'std_pace_{c}_{s}'] = laps_aggregated[f'std_pace_{c}_{s}'].fillna(0)
            laps_aggregated[f'deg_rate_{c}_{s}'] = laps_aggregated[f'deg_rate_{c}_{s}'].fillna(0)
    
    # Correct datatypes
    laps_aggregated['race_id'] = laps_aggregated['race_id'].astype(int)
    laps_aggregated['driver_id'] = laps_aggregated['driver_id'].astype(int)

    # Save file
    laps_aggregated.to_csv(save_path, encoding='utf-8', index=False)
    print("   Laps cleaned")


# --------------------------------------------------------------------------------
# Weather

def clean_weather():

    print("   Cleaning Weather...")

    # Init variables
    raw_file_name = 'weather_fp3_clean.csv'
    clean_file_name1 = 'laps_clean.csv'
    clean_file_name2 = 'weather_qualifying_clean.csv'
    load_path = os.path.join(DATA_FOLDER_PATH, raw_file_name)
    save_path1 = os.path.join(CLEAN_FOLDER_PATH, clean_file_name1)
    save_path2 = os.path.join(CLEAN_FOLDER_PATH, clean_file_name2)

    # Load file
    weather = pd.read_csv(load_path)

    # Fill unknown race ID (70th anniversary)
    results = pd.read_csv(os.path.join(CLEAN_FOLDER_PATH, 'race_results_clean_2018+.csv'))
    race_id_70th = results.loc[results['circuit_name'] == '70th Anniversary', 'race_id'].iloc[0]
    weather['race_id'] = weather['race_id'].replace('unknown', race_id_70th)

    # Filter for FP3 data
    weather_fp3 = pd.DataFrame()
    for race_id in weather['race_id'].unique():
        race_data = weather[weather['race_id'] == race_id]
        
        # Try to get FP3 data first
        fp3_data = race_data[race_data['session'] == 'FP3']
        if not fp3_data.empty:
            weather_fp3 = pd.concat([weather_fp3, fp3_data], ignore_index=True)
            continue
            
        # If no FP3, try FP2
        fp2_data = race_data[race_data['session'] == 'FP2']
        if not fp2_data.empty:
            weather_fp3 = pd.concat([weather_fp3, fp2_data], ignore_index=True)
            continue
            
        # If no FP2, try FP1
        fp1_data = race_data[race_data['session'] == 'FP1']
        if not fp1_data.empty:
            weather_fp3 = pd.concat([weather_fp3, fp1_data], ignore_index=True)

    # Filter for Qualifying weather data
    weather_qualifying = weather[weather['session'] == 'Qualifying']

    # Correct datatypes
    weather_fp3['race_id'] = weather_fp3['race_id'].astype(int)
    weather_qualifying['race_id'] = weather_qualifying['race_id'].astype(int)

    # Drop excess columns
    weather_fp3.drop('session', axis=1, inplace=True)
    weather_qualifying.drop('session', axis=1, inplace=True)
 
    # Save files
    weather_fp3.to_csv(save_path1, encoding='utf-8', index=False)
    weather_qualifying.to_csv(save_path2, encoding='utf-8', index=False)
    print("   Weather cleaned")


# --------------------------------------------------------------------------------
# Flags

def clean_flags():

    print("   Cleaning Flags...")

    # Init variables
    raw_file_name = 'flag_results_raw.csv'
    clean_file_name = 'flags_clean.csv'
    load_path = os.path.join(DATA_FOLDER_PATH, raw_file_name)
    save_path = os.path.join(CLEAN_FOLDER_PATH, clean_file_name)

    # Load file
    flags = pd.read_csv(load_path)

    # Fill unknown race ID (70th anniversary)
    results = pd.read_csv(os.path.join(CLEAN_FOLDER_PATH, 'race_results_clean_2018+.csv'))
    race_id_70th = results.loc[results['circuit_name'] == '70th Anniversary', 'race_id'].iloc[0]
    flags['race_id'] = flags['race_id'].replace('unknown', race_id_70th)

    # Filter for only race data
    flags_filtered = flags[flags['session'] == 'Race']

    # Add circuit id
    results = results[['race_id', 'circuit_id']].drop_duplicates()
    flags_filtered['race_id'] = flags_filtered['race_id'].astype(int)
    flags_filtered = flags_filtered.merge(results, on='race_id', how='inner')

    # Aggregate flag data
    flags_sorted = flags_filtered.sort_values('race_id')

    # Init lists
    race_ids = []
    circuit_ids = []
    yellow_flag_probs = []
    double_yellow_probs = []
    red_flag_probs = []
    safety_car_probs = []
    vsc_probs = []
    avg_yellow_counts = []
    avg_double_yellow_counts = []
    avg_red_counts = []
    avg_safety_car_deployments = []
    avg_vsc_deployments = []
    avg_sc_laps = []
    avg_vsc_laps = []

    # Bayesian smoothing parameter
    k = 1

    # For each circuit, calculate probabilities and averages using historical data
    for circuit_id in flags_sorted['circuit_id'].unique():
        circuit_data = flags_sorted[flags_sorted['circuit_id'] == circuit_id].sort_values('race_id')
        
        # For each race at this circuit, use data from previous races only to avoid leakage
        for i, (race_id, race_data) in enumerate(circuit_data.groupby('race_id')):
            
            # Get all races before current race at this circuit
            historical_data = circuit_data[circuit_data['race_id'] < race_id]
            
            # Store race_id and circuit_id for this specific prediction
            race_ids.append(race_id)
            circuit_ids.append(circuit_id)
            
            if len(historical_data) == 0:
                
                # First race at this circuit, will be filled with global stats later
                yellow_flag_probs.append(None)
                double_yellow_probs.append(None)
                red_flag_probs.append(None)
                safety_car_probs.append(None)
                vsc_probs.append(None)
                avg_yellow_counts.append(None)
                avg_double_yellow_counts.append(None)
                avg_red_counts.append(None)
                avg_safety_car_deployments.append(None)
                avg_vsc_deployments.append(None)
                avg_sc_laps.append(None)
                avg_vsc_laps.append(None)
            
            else:
                # Calculate probabilities with Bayesian smoothing
                total_races = len(historical_data)
                
                # Yellow flag probability
                yellow_races = len(historical_data[historical_data['flag_yellow_count'] > 0])
                yellow_flag_probs.append((yellow_races + k) / (total_races + 2*k))
                
                # Double yellow probability
                double_yellow_races = len(historical_data[historical_data['flag_double_yellow_count'] > 0])
                double_yellow_probs.append((double_yellow_races + k) / (total_races + 2*k))
                
                # Red flag probability
                red_races = len(historical_data[historical_data['flag_red_count'] > 0])
                red_flag_probs.append((red_races + k) / (total_races + 2*k))
                
                # Safety car probability
                sc_races = len(historical_data[historical_data['safety_car_deployments'] > 0])
                safety_car_probs.append((sc_races + k) / (total_races + 2*k))
                
                # VSC probability
                vsc_races = len(historical_data[historical_data['virtual_safety_car_deployments'] > 0])
                vsc_probs.append((vsc_races + k) / (total_races + 2*k))
                
                # Averages
                avg_yellow_counts.append(historical_data['flag_yellow_count'].mean())
                avg_double_yellow_counts.append(historical_data['flag_double_yellow_count'].mean())
                avg_red_counts.append(historical_data['flag_red_count'].mean())
                avg_safety_car_deployments.append(historical_data['safety_car_deployments'].mean())
                avg_vsc_deployments.append(historical_data['virtual_safety_car_deployments'].mean())
                avg_sc_laps.append(historical_data['total_sc_laps'].mean())
                avg_vsc_laps.append(historical_data['total_vsc_laps'].mean())

    # Create summary
    circuit_flag_stats = pd.DataFrame({
        'race_id': race_ids,
        'circuit_id': circuit_ids,
        'yellow_flag_prob': yellow_flag_probs,
        'double_yellow_prob': double_yellow_probs,
        'red_flag_prob': red_flag_probs,
        'safety_car_prob': safety_car_probs,
        'vsc_prob': vsc_probs,
        'avg_yellow_count': avg_yellow_counts,
        'avg_double_yellow_count': avg_double_yellow_counts,
        'avg_red_count': avg_red_counts,
        'avg_safety_car_deployments': avg_safety_car_deployments,
        'avg_vsc_deployments': avg_vsc_deployments,
        'avg_sc_laps': avg_sc_laps,
        'avg_vsc_laps': avg_vsc_laps
    })

    # Calculate global stats for imputation
    global_stats = {
        'yellow_flag_prob': (flags_sorted['flag_yellow_count'] > 0).mean(),
        'double_yellow_prob': (flags_sorted['flag_double_yellow_count'] > 0).mean(),
        'red_flag_prob': (flags_sorted['flag_red_count'] > 0).mean(),
        'safety_car_prob': (flags_sorted['safety_car_deployments'] > 0).mean(),
        'vsc_prob': (flags_sorted['virtual_safety_car_deployments'] > 0).mean(),
        'avg_yellow_count': flags_sorted['flag_yellow_count'].mean(),
        'avg_double_yellow_count': flags_sorted['flag_double_yellow_count'].mean(),
        'avg_red_count': flags_sorted['flag_red_count'].mean(),
        'avg_safety_car_deployments': flags_sorted['safety_car_deployments'].mean(),
        'avg_vsc_deployments': flags_sorted['virtual_safety_car_deployments'].mean(),
        'avg_sc_laps': flags_sorted['total_sc_laps'].mean(),
        'avg_vsc_laps': flags_sorted['total_vsc_laps'].mean(),
    }

    # Fill missing values with global stast
    for col in global_stats.keys():
        circuit_flag_stats[col] = circuit_flag_stats[col].fillna(global_stats[col])

    # Save file
    circuit_flag_stats.to_csv(save_path, encoding='utf-8', index=False)
    print("   Flags cleaned")