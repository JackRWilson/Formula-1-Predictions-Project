# Jack Wilson
# 12/17/2025
# This file collects all data from the FastF1 API

# --------------------------------------------------------------------------------

# Import modules
import pandas as pd
import numpy as np
import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.utils.utils import load_id_map, save_id_map
from src.utils.project_functions import constructor_mapping

data_folder_path = os.path.join(PROJECT_ROOT, 'data/raw')
links_2001_2017_path = os.path.join(PROJECT_ROOT, 'data/raw/links_2001_2017.pkl')
links_2018_path = os.path.join(PROJECT_ROOT, 'data/raw/links_2018+.pkl')

# --------------------------------------------------------------------------------
# Laps

# Load maps
driver_id_map = pd.read_pickle('../data/raw/driver_id_map.pkl')
race_id_map = pd.read_pickle('../data/raw/race_id_map.pkl')
circuit_id_map = pd.read_pickle('../data/raw/circuit_id_map.pkl')
driver_code_map = pd.read_pickle('../data/raw/driver_code_map.pkl')

code_to_name_map = {code: name for name, code in driver_code_map.items()}



# Compound stat function
def compute_compound_stats(comp_data, compound):
    if comp_data.empty:
        return {
            f'avg_pace_{compound.lower()}': np.nan,
            f'std_pace_{compound.lower()}': np.nan,
            f'laps_on_{compound.lower()}': 0,
            f'deg_rate_{compound.lower()}': np.nan,
        }
    
    lap_times = comp_data['LapTime'].values
    lap_numbers = comp_data['LapNumber'].values
    
    return {
        f'avg_pace_{compound.lower()}': np.nanmean(lap_times),
        f'std_pace_{compound.lower()}': np.nanstd(lap_times),
        f'laps_on_{compound.lower()}': len(lap_times),
        f'deg_rate_{compound.lower()}': get_degradation_rate(lap_times, lap_numbers),
    }

# Aggregation function
def process_file(filepath):
    """Process a single laps file and return aggregated driver stats"""
    print(f"Processing: {filepath.name}")
    
    fh = pd.read_parquet(filepath)
    
    # Filter rows
    filtered_data = fh[
        (fh['TrackStatus'] == '1') # No flags
        & fh['PitOutTime'].isna() # Not an OUT lap
        & fh['PitInTime'].isna() # Not an IN lap
        & fh['IsAccurate'] == True # Full lap completed and is accurate
        & (fh['LapTime'] < fh['LapTime'].quantile(0.95)) # Get rid of outliers
    ].copy()
    
    if len(filtered_data) == 0:
        print(f"    No data after filtering")
        return pd.DataFrame()
    
    # Convert lap times
    filtered_data['LapTime'] = filtered_data['LapTime'].dt.total_seconds()
    filtered_data = filtered_data[filtered_data['LapTime'].notna()].copy()
    
    # Map driver codes and add metadata
    filtered_data['driver_id'] = filtered_data['Driver'].map(code_to_name_map).map(driver_id_map)
    if 'race_id' in fh.columns:
        filtered_data['race_id'] = fh.loc[filtered_data.index, 'race_id']
    if 'session' in fh.columns:
        filtered_data['session'] = fh.loc[filtered_data.index, 'session']
    
    # Extract year from filename for compound mapping
    year = int(filepath.stem.split('_')[0])
    
    # Apply compound mapping for 2018
    if year == 2018:
        compound_mapping = {
            'SUPERSOFT': 'SOFT',
            'HYPERSOFT': 'SOFT',
            'ULTRASOFT': 'SOFT',
            'SOFT': 'MEDIUM',
            'MEDIUM': 'HARD',
            'HARD': 'HARD'
        }
        filtered_data['Compound'] = filtered_data['Compound'].map(compound_mapping).fillna(filtered_data['Compound'])
    
    # Aggregate by driver
    compounds = ['SOFT', 'MEDIUM', 'HARD', 'INTERMEDIATE', 'WET']
    summaries = []
    
    for driver, group in filtered_data.groupby('Driver'):
        summary = {'driver_name': driver}
        
        # Add metadata from first row
        for col in ['driver_id', 'race_id', 'session']:
            if col in group.columns:
                summary[col] = group[col].iloc[0]
        
        # Add compound stats
        for comp in compounds:
            comp_data = group[group['Compound'] == comp]
            summary.update(compute_compound_stats(comp_data, comp))
        
        summaries.append(summary)
    
    result = pd.DataFrame(summaries)
    print(f"    Created {len(result)} driver records")
    return result

# Main processing loop
fastf1_dir = Path('../data/raw/fastf1')
laps_files = sorted(fastf1_dir.glob('*_laps.parquet'))

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