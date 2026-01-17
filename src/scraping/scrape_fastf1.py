# Jack Wilson
# 12/17/2025
# This file collects all data from the FastF1 API

# --------------------------------------------------------------------------------
# Import modules

import pandas as pd
import os, sys, gc, time, subprocess
import fastf1
import logging
from pathlib import Path

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.utils.utils import load_id_map, save_id_map, print_progress_bar, aggregate_columns
from src.utils.project_functions import check_new_urls, handle_successful_urls, process_lap_file, process_flag_file

DATA_FOLDER_PATH = os.path.join(PROJECT_ROOT, 'data/raw')
LINKS_2001_2017_PATH = os.path.join(PROJECT_ROOT, 'data/raw/links_2001_2017.pkl')
LINKS_2018_PATH = os.path.join(PROJECT_ROOT, 'data/raw/links_2018+.pkl')
SUCCESSFUL_URL_TEMP_PATH = os.path.join(PROJECT_ROOT, 'data/successful/successful_urls.pkl')
FASTF1_PATH = os.path.join(PROJECT_ROOT, 'data/raw/fastf1')
CACHE_PATH = os.path.join(PROJECT_ROOT, 'data/cache')
os.makedirs(FASTF1_PATH, exist_ok=True)
os.makedirs(CACHE_PATH, exist_ok=True)


# --------------------------------------------------------------------------------
# All FastF1 Data

def collect_fastf1_data():

    # Establish sessions
    sessions = ['FP1', 'FP2', 'FP3', 'Qualifying', 'Race']

    # Passed as args when inside subprocess
    if len(sys.argv) > 1 and sys.argv[1] == "--race":
        if len(sys.argv) > 7:
            specific_session = sys.argv[7]
            session_list = [specific_session]
        else:
            session_list = sessions

        # Collect variables
        year = int(sys.argv[2])
        gp = sys.argv[3].replace('_', ' ')
        race_id_value = sys.argv[4]
        cache_dir = sys.argv[5]
        output_dir = sys.argv[6]

        # Set up cache and logging
        fastf1.Cache.enable_cache(cache_dir)
        logging.getLogger('fastf1').setLevel(logging.CRITICAL)

        # Iterate through sessions
        for s in session_list:
            try:
                gc.collect()
                session = fastf1.get_session(year, gp, s)
                session.load(laps=True, telemetry=False, weather=True, messages=True)

                # Extract data
                laps = getattr(session, 'laps', pd.DataFrame())
                weather = getattr(session, 'weather_data', pd.DataFrame())
                messages = getattr(session, 'race_control_messages', pd.DataFrame())

                # Create race_id and session columns
                for df in [laps, weather, messages]:
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        df["race_id"] = race_id_value
                        df["session"] = s

                # Save as parquet
                prefix = f"{output_dir}/{year}_{gp}_{s}"
                if not laps.empty:
                    laps.to_parquet(f"{prefix}_laps.parquet")
                if not weather.empty:
                    weather.to_parquet(f"{prefix}_weather.parquet")
                if not messages.empty:
                    messages.to_parquet(f"{prefix}_messages.parquet")

                # Clean up session
                del session
                gc.collect()
            except:
                continue
        sys.exit(0)

    print("\nCollecting FastF1 data...")
    
    # Establish paths
    urls = load_id_map(LINKS_2018_PATH)
    race_id_map = load_id_map(os.path.join(PROJECT_ROOT, 'data/raw/race_id_map.pkl'))
    SUCCESSFUL_URL_PATH = os.path.join(PROJECT_ROOT, 'data/successful/successful_urls_fastf1.pkl')

    # Check for new URLs
    new_urls = check_new_urls(urls, SUCCESSFUL_URL_PATH, from_file=False)
    if len(new_urls) == 0:
        print(f"FastF1 collection complete\n")
        return
    print(f"   Found {len(new_urls)} new links...")
    total_races = len(new_urls)
    total_sessions = total_races * len(sessions)
    print(f"   Number of sessions to process: {total_sessions}")

    # Establish successful and ID variables
    successful_urls = []
    failed_urls = []
    current_session_idx = 0
    print(f"\n   Processing {total_races} races ({total_sessions} sessions)...")

    # Iterate through new URLs
    for _, url in enumerate(new_urls, start=1):
        year = int(url.split('/')[5])
        gp = (
            url.split('/')[8]
            .replace('-', ' ')
            .title()
            .replace('Emilia Romagna', 'Emilia-Romagna')
        )
        race_key = f"{gp}_{year}"
        race_id_value = race_id_map.get(race_key, "unknown")
        race_failed = False

        # Launch a subprocess for each session within the race
        for session_name in sessions:
            current_session_idx += 1
            print_progress_bar(current_session_idx, total_sessions)
            result = subprocess.run(
                [sys.executable, __file__, "--race", str(year), gp.replace(' ', '_'), str(race_id_value), CACHE_PATH, FASTF1_PATH, session_name],
                check=False,
                stdout=sys.stdout,
                stderr=sys.stderr,
            )
            if result.returncode != 0:
                race_failed = True 
        
        # Handle failed / successful URLs
        if not race_failed:
            successful_urls.append(url)
        else:
            failed_urls.append(url)
        time.sleep(1)

    # Display failed URLs
    print("\n\n")
    if len(failed_urls) > 0:
        print(f"   Failed to process {len(failed_urls)} links:")
        for failed in failed_urls:
            print(f"      - {failed}")

    # Handle successful url file
    save_id_map(SUCCESSFUL_URL_TEMP_PATH, successful_urls)
    handle_successful_urls(SUCCESSFUL_URL_PATH, SUCCESSFUL_URL_TEMP_PATH)

    print(f"FastF1 collection complete\n")

# Define main for subprocess
if __name__ == "__main__":
    collect_fastf1_data()


# --------------------------------------------------------------------------------
# Laps

def aggregate_laps():
    
    print("\nAggregating lap data...")

    # Load paths
    AGG_LAPS_PATH = os.path.join(PROJECT_ROOT, 'data/raw/lap_results_raw.csv')
    DRIVER_ID_PATH = os.path.join(PROJECT_ROOT, 'data/raw/driver_id_map.pkl')
    DRIVER_CODE_PATH = os.path.join(PROJECT_ROOT, 'data/raw/driver_code_map.pkl')
    SUCCESSFUL_URL_PATH = os.path.join(PROJECT_ROOT, 'data/successful/successful_urls_laps.pkl')
    FASTF1_PATH_FORMAT = Path(FASTF1_PATH)
    
    try:
        # Load maps
        lap_results = pd.read_csv(AGG_LAPS_PATH)
        driver_id_map = load_id_map(DRIVER_ID_PATH)
        driver_code_map = load_id_map(DRIVER_CODE_PATH)
        code_to_name_map = {code: name for name, code in driver_code_map.items()}
        
        # Init lists
        results_list = []
        successful_urls = []
        failed_files = []

        # Glob all new lap files
        print("   Checking for new laps files...")
        existing_urls = load_id_map(SUCCESSFUL_URL_PATH)
        existing_urls_set = set([Path(url) for url in existing_urls])
        all_laps_files = sorted(FASTF1_PATH_FORMAT.glob('*_laps.parquet'))
        laps_files = [fp for fp in all_laps_files if fp not in existing_urls_set]
        total_files = len(laps_files)
        if total_files == 0:
            print("   No new files found")
            print(f"Lap aggregation complete\n")
            return
        print(f"   Found {len(laps_files)} new files...\n")

        # Process each lap file
        print(f"   Aggregating {len(laps_files)} files...")
        for i, filepath in enumerate(laps_files, start=1):
            try:
                file_result = process_lap_file(filepath, code_to_name_map=code_to_name_map, driver_id_map=driver_id_map)
                if not file_result.empty:
                    results_list.append(file_result)
                    successful_urls.append(filepath)
            except Exception as e:
                print(f"   Warning: Failed to process {filepath.name}: {e}")
                failed_files.append(filepath)
                continue

            # Update progress bar
            print_progress_bar(i, total_files)

        # Concat all results
        if results_list:
            new_lap_results = pd.concat(results_list, ignore_index=True)
        else:
            new_lap_results = pd.DataFrame()
        
        # Append new results
        all_lap_results = pd.concat([lap_results, new_lap_results], ignore_index=True)
        print(f"\n   Shape: {all_lap_results.shape}")
        if failed_files:
            print(f"   Failed files: {len(failed_files)}")
        print()
        
        # Handle successful url file
        save_id_map(SUCCESSFUL_URL_TEMP_PATH, successful_urls)
        handle_successful_urls(SUCCESSFUL_URL_PATH, SUCCESSFUL_URL_TEMP_PATH)

        # Save csv
        all_lap_results.to_csv(AGG_LAPS_PATH, encoding='utf-8', index=False)
        print(f"Lap aggregation complete\n")
    
    except Exception as e:
        print(f"Error during lap aggregation: {e}")
        raise


# --------------------------------------------------------------------------------
# Weather

def aggregate_weather():
    
    print("\nAggregating weather data...")

    # Load paths
    AGG_WEATHER_PATH = os.path.join(PROJECT_ROOT, 'data/raw/weather_results_raw.csv')
    SUCCESSFUL_URL_PATH = os.path.join(PROJECT_ROOT, 'data/successful/successful_urls_weather.pkl')
    FASTF1_PATH_FORMAT = Path(FASTF1_PATH)
    
    try:
        # Load maps
        weather_results = pd.read_csv(AGG_WEATHER_PATH)
    
        # Init lists
        results_list = []
        successful_urls = []
        failed_files = []

        # Init columns to aggregate
        numeric_columns = ['AirTemp', 'TrackTemp', 'WindSpeed', 'Humidity', 'Pressure']
        boolean_columns = ['Rainfall']
        string_columns = ['race_id', 'session']

        # Glob all new weather files
        print("   Checking for new weather files...")
        existing_urls = load_id_map(SUCCESSFUL_URL_PATH)
        existing_urls_set = set([Path(url) for url in existing_urls])
        all_weather_files = sorted(FASTF1_PATH_FORMAT.glob('*_weather.parquet'))
        weather_files = [fp for fp in all_weather_files if fp not in existing_urls_set]
        total_files = len(weather_files)
        if total_files == 0:
            print("   No new files found")
            print(f"Weather aggregation complete\n")
            return
        print(f"   Found {len(weather_files)} new files...\n")

        # Aggregate each weather file
        print(f"   Aggregating {len(weather_files)} files...")
        for i, filepath in enumerate(weather_files, start=1):
            try:
                fh = pd.read_parquet(filepath)
                file_result = aggregate_columns(df=fh, columns=numeric_columns, boolean_columns=boolean_columns, string_columns=string_columns)
                if not file_result.empty:
                    results_list.append(file_result)
                    successful_urls.append(filepath)
            except Exception as e:
                print(f"   Warning: Failed to process {filepath.name}: {e}")
                failed_files.append(filepath)
                continue

            # Update progress bar
            print_progress_bar(i, total_files)

        # Concat all results
        if results_list:
            new_weather_results = pd.concat(results_list, ignore_index=True)
        else:
            new_weather_results = pd.DataFrame()
        
        # Append new results
        all_weather_results = pd.concat([weather_results, new_weather_results], ignore_index=True)
        print(f"\n   Shape: {all_weather_results.shape}")
        if failed_files:
            print(f"   Failed files: {len(failed_files)}")
        print()
        
        # Handle successful url file
        save_id_map(SUCCESSFUL_URL_TEMP_PATH, successful_urls)
        handle_successful_urls(SUCCESSFUL_URL_PATH, SUCCESSFUL_URL_TEMP_PATH)

        # Save csv
        all_weather_results.to_csv(AGG_WEATHER_PATH, encoding='utf-8', index=False)
        print(f"Weather aggregation complete\n")
    
    except Exception as e:
        print(f"Error during weather aggregation: {e}")
        raise

# --------------------------------------------------------------------------------
# Flags

def aggregate_flags():
    
    print("\nAggregating flag data...")

    # Load paths
    AGG_FLAG_PATH = os.path.join(PROJECT_ROOT, 'data/raw/flag_results_raw.csv')
    SUCCESSFUL_URL_PATH = os.path.join(PROJECT_ROOT, 'data/successful/successful_urls_laps.pkl')
    FASTF1_PATH_FORMAT = Path(FASTF1_PATH)
    
    try:
        # Load maps
        flag_results = pd.read_csv(AGG_FLAG_PATH)
        
        # Init lists
        results_list = []
        successful_urls = []
        failed_files = []

        # Init columns to aggregate
        basic_count_flags = ['YELLOW', 'DOUBLE YELLOW', 'RED', 'CLEAR']
        string_columns = ['race_id', 'session']

        # Glob all new flag files
        print("   Checking for new flag files...")
        existing_urls = load_id_map(SUCCESSFUL_URL_PATH)
        existing_urls_set = set([Path(url) for url in existing_urls])
        all_flag_files = sorted(FASTF1_PATH_FORMAT.glob('*_messages.parquet'))
        flag_files = [fp for fp in all_flag_files if fp not in existing_urls_set]
        total_files = len(flag_files)
        if total_files == 0:
            print("   No new files found")
            print(f"Flag aggregation complete\n")
            return
        print(f"   Found {len(flag_files)} new files...\n")

        # Process each flag file
        print(f"   Aggregating {len(flag_files)} files...")
        for i, filepath in enumerate(flag_files, start=1):
            try:
                file_result = process_flag_file(filepath, basic_count_flags=basic_count_flags, string_columns=string_columns)
                if not file_result.empty:
                    results_list.append(file_result)
                    successful_urls.append(filepath)
            except Exception as e:
                print(f"   Warning: Failed to process {filepath.name}: {e}")
                failed_files.append(filepath)
                continue

            # Update progress bar
            print_progress_bar(i, total_files)

        # Concat all results
        if results_list:
            new_flag_results = pd.concat(results_list, ignore_index=True)
        else:
            new_flag_results = pd.DataFrame()
        
        # Append new results
        all_flag_results = pd.concat([flag_results, new_flag_results], ignore_index=True)
        print(f"\n   Shape: {all_flag_results.shape}")
        if failed_files:
            print(f"   Failed files: {len(failed_files)}")
        print()
        
        # Handle successful url file
        save_id_map(SUCCESSFUL_URL_TEMP_PATH, successful_urls)
        handle_successful_urls(SUCCESSFUL_URL_PATH, SUCCESSFUL_URL_TEMP_PATH)

        # Save csv
        all_flag_results.to_csv(AGG_FLAG_PATH, encoding='utf-8', index=False)
        print(f"Flag aggregation complete\n")
    
    except Exception as e:
        print(f"Error during flag aggregation: {e}")
        raise