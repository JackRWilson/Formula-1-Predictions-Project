# Import modules
import os, sys, gc, time, pickle, subprocess
import fastf1
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

from src.utils.data_functions import load_id_map

# Passed as args when inside subprocess
if len(sys.argv) > 1 and sys.argv[1] == "--race":
    
    # Subprocess mode - handle a single race
    year = int(sys.argv[2])
    gp = sys.argv[3].replace('_', ' ')
    race_id_value = sys.argv[4]
    cache_dir = sys.argv[5]
    output_dir = sys.argv[6]

    # Set up sessions
    sessions = ['FP1', 'FP2', 'FP3', 'Qualifying', 'Race']
    fastf1.Cache.enable_cache(cache_dir)

    # Main loop per session
    for s in sessions:
        try:
            gc.collect()
            session = fastf1.get_session(year, gp, s)
            session.load(laps=True, telemetry=False, weather=True, messages=True)
            print(f" Loaded {year} {gp} {s}")

            # Extract data
            laps = getattr(session, 'laps', pd.DataFrame())
            weather = getattr(session, 'weather_data', pd.DataFrame())
            messages = getattr(session, 'race_control_messages', pd.DataFrame())

            # Get race_id and session for merging later
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
            print(f" Saved {year} {gp} {s}")
            del session
            gc.collect()
            time.sleep(2)

        except Exception as e:
            print(f" Error for {year} {gp} {s}: {e}")
            time.sleep(3)
    
    # Exit subprocess after finishing a race
    sys.exit(0)

# This runs if --race isnt passed, collect directories
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

CACHE_DIR = os.path.join(PROJECT_ROOT, "data", "cache")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "fastf1")
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

urls = load_id_map(os.path.join(PROJECT_ROOT, 'data', 'raw', 'links_2018+.pkl'))
race_id_map = load_id_map(os.path.join(PROJECT_ROOT, 'data', 'raw', 'race_id_map.pkl'))

# Print info when script is run
print(f"Project root: {PROJECT_ROOT}")
print(f"Cache directory: {CACHE_DIR}")
print(f"Output directory: {OUTPUT_DIR}")
print(f"Number of URLs to process: {len(urls)}")

# Loop through all races, extract gp, year, and race ID
for idx, url in enumerate(urls):
    year = int(url.split('/')[5])
    gp = (
        url.split('/')[8]
        .replace('-', ' ')
        .title()
        .replace('Emilia Romagna', 'Emilia-Romagna')
    )
    race_key = f"{gp}_{year}"
    race_id_value = race_id_map.get(race_key, "unknown")

    # Status update
    print(f"\n=== {idx+1}/{len(urls)} | {year} {gp} ===")

    # Launch a new subprocess for this race
    subprocess.run(
        [sys.executable, __file__, "--race", str(year), gp.replace(' ', '_'), str(race_id_value), CACHE_DIR, OUTPUT_DIR],
        check=False,
    )

    # Pause briefly between races
    time.sleep(5)
