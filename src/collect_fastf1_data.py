import os
import sys
import fastf1
import pandas as pd
import time
import gc
import subprocess
import pickle
from ipynb.fs.full.data_scraping import load_id_map

# If we're inside a subprocess, these will be passed in as args
if len(sys.argv) > 1 and sys.argv[1] == "--race":
    # Subprocess mode: handle a single race
    year = int(sys.argv[2])
    gp = sys.argv[3]
    race_id_value = sys.argv[4]
    cache_dir = sys.argv[5]
    output_dir = sys.argv[6]

    sessions = ['FP1', 'FP2', 'FP3', 'Qualifying', 'Race']
    fastf1.Cache.enable_cache(cache_dir)

    for s in sessions:
        try:
            gc.collect()
            session = fastf1.get_session(year, gp, s)
            session.load(laps=True, telemetry=False, weather=True, messages=True)
            print(f" Loaded {year} {gp} {s}")

            # Extract safely
            laps = getattr(session, 'laps', pd.DataFrame())
            weather = getattr(session, 'weather_data', pd.DataFrame())
            messages = getattr(session, 'race_control_messages', pd.DataFrame())

            for df in [laps, weather, messages]:
                if isinstance(df, pd.DataFrame) and not df.empty:
                    df["race_id"] = race_id_value
                    df["session"] = s

            # Save
            prefix = f"{output_dir}/{year}_{gp}_{s}"
            if not laps.empty:
                laps.to_parquet(f"{prefix}_laps.parquet")
            if not weather.empty:
                weather.to_parquet(f"{prefix}_weather.parquet")
            if not messages.empty:
                messages.to_parquet(f"{prefix}_messages.parquet")

            print(f" Saved {year} {gp} {s}")
            del session
            gc.collect()
            time.sleep(2)

        except Exception as e:
            print(f" Error for {year} {gp} {s}: {e}")
            time.sleep(3)
    sys.exit(0)

# === Main controller ===
CACHE_DIR = "../data/cache"
OUTPUT_DIR = "../data/raw/fastf1"
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

urls = load_id_map('../data/raw/links_2018+.pkl')
race_id_map = load_id_map('../data/raw/race_id_map.pkl')

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

    print(f"\n=== {idx+1}/{len(urls)} | {year} {gp} ===")

    # Launch a new Python process for this race
    subprocess.run(
        [sys.executable, __file__, "--race", str(year), gp, str(race_id_value), CACHE_DIR, OUTPUT_DIR],
        check=False,
    )

    # Pause briefly between races
    time.sleep(5)
