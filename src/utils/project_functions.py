# Jack Wilson
# 12/4/2025
# This file defines general project specific functions and variables

# ==============================================================================================
# Import Modules
# ==============================================================================================

import pandas as pd
import numpy as np
import time, os, sys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sklearn.linear_model import LinearRegression

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.utils.utils import load_id_map, save_id_map


# ==============================================================================================
# I. Constructors Common Name Map
# ==============================================================================================

# Constructors common name mapping
constructor_mapping = {'team_id': {
    # Red Bull
    'Red Bull Racing Renault': 'Red Bull',
    'Red Bull Renault': 'Red Bull',
    'RBR Renault': 'Red Bull',
    'RBR Cosworth': 'Red Bull',
    'RBR Ferrari': 'Red Bull',
    'Red Bull Racing TAG Heuer': 'Red Bull',
    'Red Bull Racing Honda': 'Red Bull',
    'Red Bull Racing RBPT': 'Red Bull',
    'Red Bull Racing Honda RBPT': 'Red Bull',
    'Red Bull Racing': 'Red Bull',
    
    # AlphaTauri/Toro Rosso
    'Toro Rosso': 'Toro Rosso',
    'STR Ferrari': 'Toro Rosso',
    'STR Renault': 'Toro Rosso',
    'STR Cosworth': 'Toro Rosso',
    'Toro Rosso Ferrari': 'Toro Rosso',
    'Scuderia Toro Rosso Honda': 'Toro Rosso',
    'AlphaTauri Honda': 'AlphaTauri',
    'AlphaTauri RBPT': 'AlphaTauri',
    'AlphaTauri Honda RBPT': 'AlphaTauri',
    
    # Racing Bulls
    'RB Honda RBPT': 'Racing Bulls',
    
    # Ferrari
    'Ferrari': 'Ferrari',
    'Ferrari Jaguar': 'Ferrari',
    'Thin Wall Ferrari': 'Ferrari',
    
    # Mercedes
    'Mercedes': 'Mercedes',
    'Mercedes-Benz': 'Mercedes',
    
    # Aston Martin
    'Aston Martin Mercedes': 'Aston Martin',
    'Aston Martin Aramco Mercedes': 'Aston Martin',
    'Aston Butterworth': 'Aston Martin',
    'Aston Martin': 'Aston Martin',
    
    # McLaren
    'McLaren Ford': 'McLaren',
    'McLaren TAG': 'McLaren',
    'McLaren Honda': 'McLaren',
    'McLaren Peugeot': 'McLaren',
    'McLaren Renault': 'McLaren',
    'McLaren BRM': 'McLaren',
    'McLaren Mercedes': 'McLaren',
    'McLaren Serenissima': 'McLaren',
    'Mclaren BRM': 'McLaren',
    'McLaren Alfa Romeo': 'McLaren',
    
    # Williams
    'Williams Ford': 'Williams',
    'Williams Renault': 'Williams',
    'Williams Honda': 'Williams',
    'Williams Judd': 'Williams',
    'Williams BMW': 'Williams',
    'Williams Toyota': 'Williams',
    'Williams Cosworth': 'Williams',
    'Williams Mecachrome': 'Williams',
    'Williams Supertec': 'Williams',
    'Williams Mercedes': 'Williams',
    'Frank Williams Racing Cars/Williams': 'Williams',
    
    # Renault
    'Renault': 'Renault',

    # Alpine
    'Alpine Renault': 'Alpine',
    
    # Lotus
    'Lotus Renault': 'Lotus',
    'Lotus Ford': 'Lotus',
    'Lotus Climax': 'Lotus',
    'Lotus BRM': 'Lotus',
    'Lotus Honda': 'Lotus',
    'Lotus Judd': 'Lotus',
    'Lotus Lamborghini': 'Lotus',
    'Lotus Mugen Honda': 'Lotus',
    'Lotus Mercedes': 'Lotus',
    'Lotus Cosworth': 'Lotus',
    'Lotus Maserati': 'Lotus',
    'Lotus Pratt & Whitney': 'Lotus',
    
    # Force India
    'Force India Ferrari': 'Force India',
    'Force India Mercedes': 'Force India',

    # Racing Point
    'Racing Point BWT Mercedes': 'Racing Point',

    # Sauber
    'Sauber': 'Sauber',
    'Sauber Ferrari': 'Sauber',
    'Sauber Petronas': 'Sauber',
    'Sauber BMW': 'Sauber',
    'Sauber Mercedes': 'Sauber',
    'Sauber Ford': 'Sauber',
    'Kick Sauber Ferrari': 'Sauber',

    # Alfa Romeo
    'Alfa Romeo Racing Ferrari': 'Alfa Romeo',
    'Alfa Romeo Ferrari': 'Alfa Romeo',
    'Alfa Romeo': 'Alfa Romeo',
    
    # Haas
    'Haas Ferrari': 'Haas',
    'Haas F1 Team': 'Haas',
    
    # Jordan
    'Jordan Ford': 'Jordan',
    'Jordan Peugeot': 'Jordan',
    'Jordan Hart': 'Jordan',
    'Jordan Honda': 'Jordan',
    'Jordan Yamaha': 'Jordan',
    'Jordan Toyota': 'Jordan',
    'Jordan Mugen Honda': 'Jordan',
    
    # BAR
    'BAR Honda': 'BAR',
    'BAR Supertec': 'BAR',
    
    # Honda
    'Honda': 'Honda',
    
    # Benetton
    'Benetton Ford': 'Benetton',
    'Benetton BMW': 'Benetton',
    'Benetton Renault': 'Benetton',
    'Benetton Playlife': 'Benetton',
    
    # Toyota
    'Toyota': 'Toyota',
    
    # Jaguar
    'Jaguar Cosworth': 'Jaguar',
    
    # Stewart
    'Stewart Ford': 'Stewart',
    
    # BRM
    'BRM': 'BRM',
    'BRM Climax': 'BRM',

    # JBW
    'JBW Maserati': 'JBW',
    'JBW Climax': 'JBW',
    
    # Cooper
    'Cooper Climax': 'Cooper',
    'Cooper Maserati': 'Cooper',
    'Cooper Bristol': 'Cooper',
    'Cooper Castellotti': 'Cooper',
    'Cooper BRM': 'Cooper',
    'Cooper JAP': 'Cooper',
    'Cooper Alta': 'Cooper',
    'Cooper Borgward': 'Cooper',
    'Cooper Alfa Romeo': 'Cooper',
    'Cooper Ferrari': 'Cooper',
    'Cooper ATS': 'Cooper',
    'Cooper Ford': 'Cooper',
    'Cooper OSCA': 'Cooper',
    
    # Brabham
    'Brabham Climax': 'Brabham',
    'Brabham Repco': 'Brabham',
    'Brabham Ford': 'Brabham',
    'Brabham Alfa Romeo': 'Brabham',
    'Brabham BMW': 'Brabham',
    'Brabham BRM': 'Brabham',
    'Brabham Judd': 'Brabham',
    'Brabham Yamaha': 'Brabham',
    
    # Maserati
    'Maserati': 'Maserati',
    'Maserati Offenhauser': 'Maserati',
    'Maserati Milano': 'Maserati',
    'Maserati-Offenhauser': 'Maserati',
    'Maserati OSCA': 'Maserati',
    'Maserati Plate': 'Maserati',
    
    # Ligier
    'Ligier Matra': 'Ligier',
    'Ligier Ford': 'Ligier',
    'Ligier Renault': 'Ligier',
    'Ligier Megatron': 'Ligier',
    'Ligier Mugen Honda': 'Ligier',
    
    # Tyrrell
    'Tyrrell Ford': 'Tyrrell',
    'Tyrrell Renault': 'Tyrrell',
    'Tyrrell Honda': 'Tyrrell',
    'Tyrrell Yamaha': 'Tyrrell',
    'Tyrrell Ilmor': 'Tyrrell',
    
    # Arrows/Footwork
    'Arrows Ford': 'Arrows',
    'Arrows BMW': 'Arrows',
    'Arrows Megatron': 'Arrows',
    'Arrows Yamaha': 'Arrows',
    'Arrows Supertec': 'Arrows',
    'Arrows Asiatech': 'Arrows',
    'Arrows Cosworth': 'Arrows',
    'Arrows': 'Arrows',
    'Footwork Ford': 'Footwork',
    'Footwork Hart': 'Footwork',
    'Footwork Mugen Honda': 'Footwork',
    'Footwork Porsche': 'Footwork',
    
    # Vanwall
    'Vanwall': 'Vanwall',
    
    # Wolf
    'Wolf Ford': 'Wolf',
    'Wolf-Williams': 'Wolf',
    
    # Lola
    'Lola Ford': 'Lola',
    'Lola Lamborghini': 'Lola',
    'Lola Climax': 'Lola',
    'Lola BMW': 'Lola',
    'Lola Hart': 'Lola',
    'Lola Ferrari': 'Lola',

    # March
    'March Ford': 'March',
    'March Judd': 'March',
    'March Ilmor': 'March',
    'March Alfa Romeo': 'March',

    # Minardi
    'Minardi Ford': 'Minardi',
    'Minardi Ferrari': 'Minardi',
    'Minardi Lamborghini': 'Minardi',
    'Minardi Asiatech': 'Minardi',
    'Minardi Cosworth': 'Minardi',
    'Minardi Fondmetal': 'Minardi',
    'Minardi European': 'Minardi',
    'Minardi Hart': 'Minardi',
    'Minardi Motori Moderni': 'Minardi',
    
    # LDS
    'LDS Alfa Romeo': 'LDS',
    'LDS Climax': 'LDS',
    'LDS Repco': 'LDS',

    # Porche
    'Porsche (F2)': 'Porsche',
    'Porsche': 'Porsche',
    'Behra-Porsche': 'Porsche',

    # Scirocco
    'Scirocco BRM': 'Scirocco',
    'Scirocco Climax': 'Scirocco',

    # AFM
    'AFM Kuchen': 'AFM',
    'AFM BMW': 'AFM',
    'AFM Bristol': 'AFM',

    # ATS
    'ATS Ford': 'ATS',
    'ATS': 'ATS',
    'ATS BMW': 'ATS',
    'Derrington-Francis ATS': 'ATS',

    # Leyton House
    'Leyton House Judd': 'Leyton House',
    'Leyton House Ilmor': 'Leyton House',

    # Prost
    'Prost Mugen Honda': 'Prost',
    'Prost Peugeot': 'Prost',
    'Prost Acer': 'Prost',

    # Dallara
    'Dallara Judd': 'Dallara',
    'Dallara Ferrari': 'Dallara',
    'Dallara Ford': 'Dallara',

    # Larrousse
    'Larrousse Lamborghini': 'Larrousse',
    'Larrousse Ford': 'Larrousse',

    # Osella
    'Osella Ford': 'Osella',
    'Osella Alfa Romeo': 'Osella',
    'Osella': 'Osella',
    'Osella Hart': 'Osella',

    # Kurtis Kraft
    'Kurtis Kraft Offenhauser': 'Kurtis Kraft',
    'Kurtis Kraft Novi': 'Kurtis Kraft',
    'Kurtis Kraft Cummins': 'Kurtis Kraft',

    # Marussia
    'Marussia Cosworth': 'Marussia',
    'Marussia Ferrari': 'Marussia',

    # Gordini
    'Simca-Gordini': 'Gordini',
    'Gordini': 'Gordini',

    # Connaught
    'Connaught Lea Francis': 'Connaught',
    'Connaught Alta': 'Connaught',

    # Eagle
    'Eagle Climax': 'Eagle',
    'Eagle Weslake': 'Eagle',

    # RAM
    'RAM Ford': 'RAM',
    'RAM Hart': 'RAM',

    # Shadow
    'Shadow Ford': 'Shadow',
    'Shadow Matra': 'Shadow',

    # Matra
    'Matra Ford': 'Matra',
    'Matra': 'Matra',
    'Matra Cosworth': 'Matra',
    'Matra BRM': 'Matra',

    # ERA
    'ERA': 'ERA',
    'ERA Bristol': 'ERA',

    # Spirit
    'Spirit Honda': 'Spirit',   
    'Spirit Hart': 'Spirit',

    # Frazer Nash
    'Frazer Nash': 'Frazer Nash',
    'Frazer Nash Bristol': 'Frazer Nash',

    # Emeryson
    'Emeryson Alta': 'Emeryson',
    'Emeryson Climax': 'Emeryson',

    # De Tomaso
    'De Tomaso OSCA': 'De Tomaso',
    'De Tomaso Alfa Romeo': 'De Tomaso',
    'De Tomaso Ford': 'De Tomaso',

    # Gilby
    'Gilby Climax': 'Gilby',
    'Gilby BRM': 'Gilby',

    # Tecno
    'Tecno': 'Tecno',
    'Tecno Cosworth': 'Tecno',

    # Ligier
    'Ligier Judd': 'Ligier',
    'Ligier Lamborghini': 'Ligier',

    # Euro Brun
    'Euro Brun Judd': 'Euro Brun',
    'Euro Brun Ford': 'Euro Brun',


    # Other
    'No Team': 'Privateer',
    'Toleman Hart': 'Toleman',       
    'Venturi Lamborghini': 'Venturi',        
    'Onyx Ford': 'Onyx',
    'AGS Ford': 'AGS',   
    'Rial Ford': 'Rial',
    'Zakspeed': 'Zakspeed',
    'Theodore Ford': 'Theodore',
    'Deidt Offenhauser': 'Deidt',
    'Sherman Offenhauser': 'Sherman',
    'Schroeder Offenhauser': 'Schroeder',
    'Kuzma Offenhauser': 'Kuzma',
    'Lesovsky Offenhauser': 'Lesovsky',
    'Watson Offenhauser': 'Watson',
    'Phillips Offenhauser': 'Phillips',
    'Epperly Offenhauser': 'Epperly',
    'Trevis Offenhauser': 'Trevis',
    'HRT Cosworth': 'HRT',
    'Virgin Cosworth': 'Virgin',
    'Caterham Renault': 'Caterham',
    'Milano Speluzzi': 'Milano',
    'Turner Offenhauser': 'Turner',
    'Alta': 'Alta',    
    'Moore Offenhauser': 'Moore',
    'Nichels Offenhauser': 'Nichels',
    'Marchese Offenhauser': 'Marchese',
    'Stevens Offenhauser': 'Stevens',
    'Langley Offenhauser': 'Langley',
    'Ewing Offenhauser': 'Ewing',   
    'Rae Offenhauser': 'Rae',
    'Olson Offenhauser': 'Olson',
    'Wetteroth Offerhauser': 'Wetteroth',
    'Snowberger Offenhauser': 'Snowberger',
    'Adams Offenhauser': 'Adams',
    'HWM Alta': 'HWM',    
    'Lancia': 'Lancia',
    'Talbot-Lago': 'Talbot-Lago',
    'BRP BRM': 'BRP',
    'Hesketh Ford': 'Hesketh',
    'Hill Ford': 'Hill',
    'Ensign Ford': 'Ensign',
    'Penske Ford': 'Penske',
    'Fittipaldi Ford': 'Fittipaldi',
    'ISO Marlboro Ford': 'ISO Marlboro',
    'Iso Marlboro Ford': 'ISO Marlboro',
    'Surtees Ford': 'Surtees',
    'Parnelli Ford': 'Parnelli',
    'Super Aguri Honda': 'Super Aguri',
    'MRT Mercedes': 'Manor',
    'Brawn Mercedes': 'Brawn',
    'Spyker Ferrari': 'Spyker',
    'MF1 Toyota': 'Midland',
    'Veritas': 'Veritas',
    'Pawl Offenhauser': 'Pawl',
    'Hall Offenhauser': 'Hall',
    'Bromme Offenhauser': 'Bromme',
    'OSCA': 'OSCA',
    'BMW': 'BMW',
    'EMW': 'EMW',
    'Pankratz Offenhauser': 'Pankratz',
    'Bugatti': 'Bugatti',
    'Klenk BMW': 'Klenk',
    'Dunn Offenhauser': 'Dunn',    
    'Elder Offenhauser': 'Elder',
    'Christensen Offenhauser': 'Christensen',
    'Sutton Offenhauser': 'Sutton',
    'Tec-Mec Maserati': 'Tec-Mec',
    'Meskowski Offenhauser': 'Meskowski',
    'Scarab': 'Scarab',
    'Ferguson Climax': 'Ferguson',
    'ENB Maserati': 'ENB',
    'Stebro Ford': 'Stebro',               
    'Shannon Climax': 'Shannon',     
    'Protos Cosworth': 'Protos',   
    'Bellasi Ford': 'Bellasi',       
    'Eifelland Ford': 'Eifelland',
    'Politoys Ford': 'Politoys',
    'Connew Ford': 'Connew',
    'Trojan Ford': 'Trojan',
    'Amon Ford': 'Amon',
    'Token Ford': 'Token',
    'Lyncar Ford': 'Lyncar',
    'Boro Ford': 'Boro',
    'Kojima Ford': 'Kojima',
    'LEC Ford': 'LEC',
    'Merzario Ford': 'Merzario',
    'Martini Ford': 'Martini',
    'Rebaque Ford': 'Rebaque',
    'AGS Motori Moderni': 'AGS',
    'Coloni Ford': 'Coloni',
    'Zakspeed Yamaha': 'Zakspeed',
    'Fondmetal Ford': 'Fondmetal',
    'Moda Judd': 'Moda',    
    'Simtek Ford': 'Simtek',
    'Pacific Ilmor': 'Pacific',
    'Forti Ford': 'Forti',
    'Lambo Lamborghini': 'Modena'
}}


# ==============================================================================================
# II. Extract Date from Website
# ==============================================================================================

def get_date(browser):
    """
    Extract date with wait and retry
    
    """
    try:
        # Wait for the element to be present
        element = WebDriverWait(browser, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[@class='Container-module_container__0e4ac']//p[contains(@class, 'display-s-bold')]"))
        )
        
        # Wait a bit for any re-rendering to finish
        time.sleep(0.5)
        
        # Get fresh reference to avoid stale element
        element = browser.find_element(By.XPATH, "//div[@class='Container-module_container__0e4ac']//p[contains(@class, 'display-s-bold')]")
        return element.text
    except Exception as e:
        print(f"Failed to extract date: {e}")
        return None


# ==============================================================================================
# III. Handle Appending or New File
# ==============================================================================================

def handle_appending(df_path, df, title, dup_subset: list = ['race_id', 'driver_id']):
    """
    Handles appending of new data onto existing file, or creating a new file


    Parameters
    ----------
    df_path : str
        Path to existing dataframe file, and where new appended dataframe will be saved
    df : DataFrame
        Dataframe with new data
    title : str
        Title of data
        Example: 'results' or 'practices'
    dup_subset : list, optional
        List of column names to look for duplicate values on
        Default: ['race_id', 'driver_id']

    """
    # Check if path exists and appends if it does
    if os.path.exists(df_path) and len(df) > 0:
        print(f"   Appending new {title} to existing file...")
        existing_df = pd.read_csv(df_path, encoding='utf-8')
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=dup_subset, keep='last')
        combined_df.to_csv(df_path, encoding='utf-8', index=False)
    elif len(df) > 0:
        print(f"   Creating new {title} file...")
        df.to_csv(df_path, encoding='utf-8', index=False)
    else:
        print(f"   No new {title} to save")


# ==============================================================================================
# IV. Handle Successful URL File
# ==============================================================================================

def handle_successful_urls(successful_urls_path, successful_urls_temp_path):
    """
    Handle successful url file
    
    """
    print("   Updating successful URL file...")
    try:
        if os.path.exists(successful_urls_temp_path):
            new_urls = load_id_map(successful_urls_temp_path)

            # Load existing successful URLs
            if os.path.exists(successful_urls_path):
                existing_urls = load_id_map(successful_urls_path)
                existing_set = set(existing_urls)
                combined_urls = existing_urls + [url for url in new_urls if url not in existing_set]
            else:
                combined_urls = new_urls
        
            # Save combined URLs and remove temp file
            save_id_map(successful_urls_path, combined_urls)
            os.remove(successful_urls_temp_path)
            print(f"   Added {len(new_urls)} successful URLs to list")
        else:
            print("   No new successful URLs found to add")
            
    except Exception as e:
        print(f"   Failed to handle successful URL file: {e}")


# ==============================================================================================
# V. Check for new URLs
# ==============================================================================================

def check_new_urls(current_path, successful_path, from_file=True):
    """
    Checks for new URLs between successful and current.

    Parameters
    ----------
    current_path : str, list
        Path to file with current URLs or the list of URLs
    successful_path : str
        Path to file with successful URLs
    from_file : bool, optional
        Whether current_path is a file path (True) or a list of URLs (False)
        Default: True
    
    Returns
    -------
    List of URLs that are in the current_path, but not the successful_path, meaning they are new

    """

    # Check for new URLs
    print("   Checking for new links...")

    if from_file:
        existing_links = load_id_map(current_path)
    else:
        existing_links = current_path

    successful_links = load_id_map(successful_path)
    successful_links_set = set(successful_links)
    urls = [url for url in existing_links if url not in successful_links_set]
    
    if len(urls) == 0:
        print("   No new links found")
    
    return urls


# ==============================================================================================
# VI. Calculate Degradation Rate
# ==============================================================================================

def get_degradation_rate(lap_times, lap_numbers):
    """
    Calculates the degradation rate of tyres based on a linear regression

    """
    valid_mask = ~np.isnan(lap_times)
    clean_lap_times = lap_times[valid_mask]
    clean_lap_numbers = lap_numbers[valid_mask]
    
    if len(clean_lap_times) > 3:
        X = np.array(clean_lap_numbers).reshape(-1, 1)
        y = np.array(clean_lap_times)
        model = LinearRegression().fit(X, y)
        return model.coef_[0]
    return np.nan


# ==============================================================================================
# VII. Calculate Compound Stats
# ==============================================================================================

def compute_compound_stats(comp_data, compound):
    """
    Calculate compound statistics

    """
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


# ==============================================================================================
# VIII. Process and Aggregate Lap Files
# ==============================================================================================

def process_file(filepath):
    """
    Process a single laps file and return aggregated driver stats
    
    """
    print(f"Processing: {filepath.name}")
    
    fh = pd.read_parquet(filepath)
    
    # Filter rows
    filtered_data = fh[
        (fh['TrackStatus'] == '1')  # No flags
        & fh['PitOutTime'].isna()  # Not an OUT lap
        & fh['PitInTime'].isna()  # Not an IN lap
        & fh['IsAccurate'] == True  # Full lap completed and is accurate
        & (fh['LapTime'] < fh['LapTime'].quantile(0.95))  # Get rid of outliers
    ].copy()
    
    if len(filtered_data) == 0:
        print(f"   No data after filtering")
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