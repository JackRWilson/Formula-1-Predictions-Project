# Jack Wilson
# 12/17/2025
# This file collects all data from the FastF1 API

# --------------------------------------------------------------------------------

# Import modules
import pandas as pd
import os, sys, gc, time, subprocess
import fastf1
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.utils.utils import load_id_map, save_id_map, print_progress_bar
from src.utils.project_functions import check_new_urls, handle_successful_urls, process_file

DATA_FOLDER_PATH = os.path.join(PROJECT_ROOT, 'data/raw')
LINKS_2001_2017_PATH = os.path.join(PROJECT_ROOT, 'data/raw/links_2001_2017.pkl')
LINKS_2018_PATH = os.path.join(PROJECT_ROOT, 'data/raw/links_2018+.pkl')
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
    SUCCESSFUL_URL_TEMP_PATH = os.path.join(PROJECT_ROOT, 'data/raw/successful_urls.pkl')
    SUCCESSFUL_URL_PATH = os.path.join(PROJECT_ROOT, 'data/raw/successful_urls_fastf1.pkl')

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
    start_time = time.time()
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
            print_progress_bar(current_session_idx, total_sessions, start_time=start_time)
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

    # Update progress bar
    print_progress_bar(total_sessions, total_sessions, start_time=start_time)
    print("\n")

    # Display failed URLs
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
    # Load maps
    driver_id_map = pd.read_pickle('../data/raw/driver_id_map.pkl')
    race_id_map = pd.read_pickle('../data/raw/race_id_map.pkl')
    circuit_id_map = pd.read_pickle('../data/raw/circuit_id_map.pkl')
    driver_code_map = pd.read_pickle('../data/raw/driver_code_map.pkl')

    code_to_name_map = {code: name for name, code in driver_code_map.items()}
    print("Collecting lap by lap data...")
    # Glob all lap files
    laps_files = sorted(fastf1_path.glob('*_laps.parquet'))

    print(f"Found {len(laps_files)} files with 'laps' in name\n")

    lap_results = pd.DataFrame()

    for filepath in laps_files:
        file_result = process_file(filepath)
        if not file_result.empty:
            lap_results = pd.concat([lap_results, file_result], ignore_index=True)

    # Count rows where all compound lap counts are 0 (null)
    compound_columns = ['laps_on_soft', 'laps_on_medium', 'laps_on_hard', 'laps_on_intermediate', 'laps_on_wet']
    all_null_count = ((lap_results[compound_columns] == 0) | lap_results[compound_columns].isna()).all(axis=1).sum()

    print(f"Rows with no laps on any compound: {all_null_count}")
    print(f"\nShape: {lap_results.shape}\n")

    lap_results.to_csv('../data/raw/lap_results_raw.csv', encoding='utf-8', index=False)