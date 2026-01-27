# Jack Wilson
# 1/27/2026
# This file cleans all merged data into final dataframes

# --------------------------------------------------------------------------------
# Import modules

import pandas as pd
import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

CLEAN_FOLDER_PATH = os.path.join(PROJECT_ROOT, 'data/clean')
INTERMEDIATE_FOLDER_PATH = os.path.join(PROJECT_ROOT, 'data/intermediate')
FINAL_FOLDER_PATH = os.path.join(PROJECT_ROOT, 'data/final')


# --------------------------------------------------------------------------------
# Pre-qualifying

def clean_pre_qual():

    print("   Cleaning pre-qualifying data...")

    # Load
    pre_qual_df = pd.read_csv(os.path.join(INTERMEDIATE_FOLDER_PATH, 'f1_data_pre_qual_raw.csv'))
    races_2001 = pd.read_csv(os.path.join(CLEAN_FOLDER_PATH, 'race_results_clean_2001-2017.csv'))

    # Calculate rolling average finish and cumulative metrics
    windows = [3, 5, 10]

    # Rename some columns for consistency
    pre_qual_df_renamed = pre_qual_df.rename(columns={'end_position': 'position', 'round_number': 'round'})

    # Combine all historical races
    all_races = pd.concat([races_2001[['driver_id', 'driver_name', 'year', 'round', 'position', 'position_status', 'points', 'team_id']], 
                        pre_qual_df_renamed[['driver_id', 'driver_name', 'year', 'round', 'position', 'position_status', 'points', 'team_id']]], 
                        ignore_index=True)

    # Sort chronologically by year and round for each driver
    all_races = all_races.sort_values(['driver_id', 'year', 'round'])

    # Calculate cumulative stats BEFORE each race
    all_races['cumulative_races'] = (
        all_races.groupby('driver_id').cumcount().shift(1).fillna(0)
    )
    all_races['cumulative_wins'] = (
        all_races.groupby('driver_id')['position']
        .transform(lambda x: (x == 1).cumsum().shift(1).fillna(0))
    )
    all_races['cumulative_podiums'] = (
        all_races.groupby('driver_id')['position']
        .transform(lambda x: x.isin([1, 2, 3]).cumsum().shift(1).fillna(0))
    )
    all_races['cumulative_points'] = (
        all_races.groupby('driver_id')['points']
        .transform(lambda x: x.cumsum().shift(1).fillna(0))
    )

    # Create rolling average finish columns, keep NA until enough races are completed
    for w in windows:
        all_races[f'avg_finish_last_{w}'] = (
            all_races.groupby('driver_id')['position']
            .transform(lambda x: x.shift(1).rolling(w, min_periods=w).mean())
        )

    # Calculate team average position from both drivers for imputing
    team_race_avg = all_races.groupby(['team_id', 'year', 'round'])['position'].mean().reset_index()
    team_race_avg = team_race_avg.rename(columns={'position': 'team_race_avg_position'})
    all_races = all_races.merge(team_race_avg, on=['team_id', 'year', 'round'], how='left')

    # Calculate rolling team averages
    for w in windows:
        all_races[f'team_avg_finish_last_{w}'] = (
            all_races.groupby('team_id')['team_race_avg_position']
            .transform(lambda x: x.shift(1).rolling(w, min_periods=1).mean())
        )

    # Filter to only include pre_qual_df races and merge the calculated features
    pre_qual_enhanced = pre_qual_df_renamed.merge(
        all_races[all_races['year'].isin(pre_qual_df_renamed['year'].unique())][
            ['driver_id', 'year', 'round', 'cumulative_races', 'cumulative_wins', 'cumulative_podiums', 
            'cumulative_points', 'avg_finish_last_3', 'avg_finish_last_5', 'avg_finish_last_10',
            'team_avg_finish_last_3', 'team_avg_finish_last_5', 'team_avg_finish_last_10']
        ],
        on=['driver_id', 'year', 'round'],
        how='left'
    )

    # Fill driver NA values with team averages
    for w in windows:
        driver_col = f'avg_finish_last_{w}'
        team_col = f'team_avg_finish_last_{w}'
        pre_qual_enhanced[driver_col] = pre_qual_enhanced[driver_col].fillna(pre_qual_enhanced[team_col])

    # Remove team average columns from final dataframe
    team_avg_cols = [f'team_avg_finish_last_{w}' for w in windows]
    pre_qual_enhanced = pre_qual_enhanced.drop(columns=team_avg_cols)

    # Add rookie flag
    first_seasons = all_races.groupby('driver_id')['year'].min().reset_index()
    first_seasons.columns = ['driver_id', 'first_season_year']

    # Merge with enhanced dataframe
    pre_qual_enhanced = pre_qual_enhanced.merge(first_seasons, on='driver_id', how='left')

    # Create rookie flag
    pre_qual_enhanced['rookie_flag'] = pre_qual_enhanced['year'] == pre_qual_enhanced['first_season_year']

    # Drop the temporary column
    pre_qual_enhanced = pre_qual_enhanced.drop(columns=['first_season_year'])

    # Calculate rates for CLAS, DNF, and NC using all races
    for status in ['CLAS', 'DNF', 'NC']:
        overall_rate = (all_races['position_status'] == status).mean()
        
        all_races[f'{status}_rate'] = (
            all_races.groupby('driver_id')['position_status']
            .apply(lambda x: (x.shift(1) == status).expanding().mean())
            .fillna(overall_rate)
            .values
        )

    # Merge the calculated rates back to pre_qual_enhanced
    pre_qual_enhanced = pre_qual_enhanced.merge(
        all_races[['driver_id', 'year', 'round', 'CLAS_rate', 'DNF_rate', 'NC_rate']],
        on=['driver_id', 'year', 'round'],
        how='left',
    )

    # Impute missing practices
    for session_num in [1, 2, 3]:
        best_time_col = f'best_time_FP{session_num}'
        recorded_time_col = f'recorded_lap_time_FP{session_num}'
        lap_count_col = f'lap_count_FP{session_num}'
        participated_col = f'participated_FP{session_num}'
        position_col = f'position_FP{session_num}'
        
        # Impute False for participated_FP* if lap_count_FP* is NA
        pre_qual_enhanced.loc[pre_qual_enhanced[lap_count_col].isna(), participated_col] = pre_qual_enhanced.loc[pre_qual_enhanced[lap_count_col].isna(), participated_col].fillna(False)

        # Impute 0 for NA rows in lap_count_FP*
        pre_qual_enhanced[lap_count_col] = pre_qual_enhanced[lap_count_col].fillna(0)
        
        # Impute False in recorded_lap_time_FP* if best_time_FP* is NA
        pre_qual_enhanced.loc[pre_qual_enhanced[best_time_col].isna(), recorded_time_col] = False
        
        for race_id, group in pre_qual_enhanced.groupby('race_id'):
            
            # Impute last place for position_FP*
            if group[position_col].isna().any():
                max_position = group[position_col].max()
                if pd.notna(max_position):
                    missing_indices = group[group[position_col].isna()].index
                    pre_qual_enhanced.loc[missing_indices, position_col] = max_position + 1
            
            # Impute best_time_FP*
            if group[best_time_col].isna().any():
                most_recent_best_time = group[best_time_col].dropna().iloc[-1] if not group[best_time_col].dropna().empty else None
                if most_recent_best_time is not None:
                    imputed_best_time = most_recent_best_time * 1.05
                    missing_indices = group[group[best_time_col].isna()].index
                    pre_qual_enhanced.loc[missing_indices, best_time_col] = imputed_best_time
        
    # Impute missing pits
    pit_time_cols = ['avg_pit_time_last_5', 'avg_pit_time_last_10']
    for col in pit_time_cols:
        
        # Fill with team average for that race (teammate)
        team_race_avg = pre_qual_enhanced.groupby(['team_id', 'race_id'])[col].transform('mean')
        pre_qual_enhanced[col] = pre_qual_enhanced[col].fillna(team_race_avg)
        
        # Fill with teams historical average
        team_historical_avg = pre_qual_enhanced.groupby('team_id')[col].transform('mean')
        pre_qual_enhanced[col] = pre_qual_enhanced[col].fillna(team_historical_avg)
        
        # Fill with race average
        race_avg = pre_qual_enhanced.groupby('race_id')[col].transform('mean')
        pre_qual_enhanced[col] = pre_qual_enhanced[col].fillna(race_avg)
        
        # Fill with overall column mean
        pre_qual_enhanced[col] = pre_qual_enhanced[col].fillna(pre_qual_enhanced[col].mean())

    # Sort columns
    pre_qual_clean = pre_qual_enhanced[[
        'race_id',
        'driver_id',
        'circuit_id',
        'team_id',
        'year',
        'round',
        'driver_name',
        'team_name',
        'cumulative_races',
        'cumulative_wins',
        'cumulative_podiums',
        'cumulative_points',
        'avg_finish_last_3',
        'avg_finish_last_5',
        'avg_finish_last_10',
        'position',  # Target
        'points',  # Remove later
        'laps_completed',  # Remove later
        'CLAS_rate',
        'NC_rate',
        'DNF_rate',
        'rookie_flag',
        'circuit_name_x',  # Rename
        'type',
        'direction',
        'length',
        'turns',
        'elevation',    
        'yellow_flag_prob',
        'double_yellow_prob',
        'red_flag_prob',
        'safety_car_prob',
        'vsc_prob',
        'avg_yellow_count',
        'avg_double_yellow_count',
        'avg_red_count',
        'avg_safety_car_deployments',
        'avg_vsc_deployments',
        'avg_sc_laps',
        'avg_vsc_laps',
        'best_time_FP1',
        'best_time_FP2',
        'best_time_FP3',
        'lap_count_FP1',
        'lap_count_FP2',
        'lap_count_FP3',
        'position_FP1',
        'position_FP2',
        'position_FP3',
        'recorded_lap_time_FP1',
        'recorded_lap_time_FP2',
        'recorded_lap_time_FP3',
        'participated_FP1',
        'participated_FP2',
        'participated_FP3',
        'avg_pit_time_last_5',
        'avg_pit_time_last_10',
        'AirTemp_mean',
        'AirTemp_min',
        'AirTemp_max',
        'AirTemp_std',
        'TrackTemp_mean',
        'TrackTemp_min',
        'TrackTemp_max',
        'TrackTemp_std',
        'WindSpeed_mean',
        'WindSpeed_min',
        'WindSpeed_max',
        'WindSpeed_std',
        'Humidity_mean',
        'Humidity_min',
        'Humidity_max',
        'Humidity_std',
        'Pressure_mean',
        'Pressure_min',
        'Pressure_max',
        'Pressure_std',
        'Rainfall_any',
        'Rainfall_mean',
        'used_soft_FP1',
        'avg_pace_soft_FP1',
        'std_pace_soft_FP1',
        'deg_rate_soft_FP1',
        'used_soft_FP2',
        'avg_pace_soft_FP2',
        'std_pace_soft_FP2',
        'deg_rate_soft_FP2',
        'used_soft_FP3',
        'avg_pace_soft_FP3',
        'std_pace_soft_FP3',
        'deg_rate_soft_FP3',   
        'used_medium_FP1',
        'avg_pace_medium_FP1',
        'std_pace_medium_FP1',
        'deg_rate_medium_FP1',
        'used_medium_FP2',
        'avg_pace_medium_FP2',
        'std_pace_medium_FP2',    
        'deg_rate_medium_FP2',
        'used_medium_FP3',
        'avg_pace_medium_FP3',
        'std_pace_medium_FP3',
        'deg_rate_medium_FP3',
        'used_hard_FP1',
        'avg_pace_hard_FP1',
        'std_pace_hard_FP1',
        'deg_rate_hard_FP1',
        'used_hard_FP2',
        'avg_pace_hard_FP2',
        'std_pace_hard_FP2',
        'deg_rate_hard_FP2',
        'used_hard_FP3',
        'avg_pace_hard_FP3',
        'std_pace_hard_FP3',
        'deg_rate_hard_FP3',
        'used_intermediate_FP1',
        'avg_pace_intermediate_FP1',
        'std_pace_intermediate_FP1',
        'deg_rate_intermediate_FP1',
        'used_intermediate_FP2',
        'avg_pace_intermediate_FP2',
        'std_pace_intermediate_FP2',
        'deg_rate_intermediate_FP2',
        'used_intermediate_FP3',
        'avg_pace_intermediate_FP3',   
        'std_pace_intermediate_FP3',
        'deg_rate_intermediate_FP3',
        'used_wet_FP1',
        'avg_pace_wet_FP1',
        'std_pace_wet_FP1',
        'deg_rate_wet_FP1',
        'used_wet_FP2',
        'avg_pace_wet_FP2',
        'std_pace_wet_FP2',
        'deg_rate_wet_FP2',
        'used_wet_FP3',
        'avg_pace_wet_FP3',
        'std_pace_wet_FP3',
        'deg_rate_wet_FP3'
    ]].rename(columns={'circuit_name_x': 'circuit_name'})

    # Correct datatypes
    pre_qual_clean['cumulative_races'] = pre_qual_clean['cumulative_races'].astype(int)
    pre_qual_clean['cumulative_wins'] = pre_qual_clean['cumulative_wins'].astype(int)
    pre_qual_clean['cumulative_podiums'] = pre_qual_clean['cumulative_podiums'].astype(int)
    pre_qual_clean['laps_completed'] = pre_qual_clean['laps_completed'].astype(int)
    pre_qual_clean['elevation'] = pre_qual_clean['elevation'].astype(int)

    for session in range(1, 4):
        pre_qual_clean[f'lap_count_FP{session}'] = pre_qual_clean[f'lap_count_FP{session}'].astype(int)
        pre_qual_clean[f'position_FP{session}'] = pre_qual_clean[f'position_FP{session}'].astype(int)
        pre_qual_clean[f'recorded_lap_time_FP{session}'] = pre_qual_clean[f'recorded_lap_time_FP{session}'].astype(bool)
        pre_qual_clean[f'participated_FP{session}'] = pre_qual_clean[f'participated_FP{session}'].astype(bool)
    
    # Save
    pre_qual_shape = pre_qual_clean.shape
    print(f"   Shape: {pre_qual_shape}")
    pre_qual_clean.to_csv(os.path.join(FINAL_FOLDER_PATH, 'f1_data_pre_qual_clean.csv'), encoding='utf-8', index=False)
    print("   Pre-qualifying data cleaned")


# --------------------------------------------------------------------------------
# Pre-race

def clean_pre_race():

    print("   Cleaning pre-race data...")

    # Load
    pre_race_df = pd.read_csv(os.path.join(INTERMEDIATE_FOLDER_PATH, 'f1_data_pre_race_raw.csv'))
    races_2001 = pd.read_csv(os.path.join(CLEAN_FOLDER_PATH, 'race_results_clean_2001-2017.csv'))

    # Calculate rolling average finish and cumulative metrics
    windows = [3, 5, 10]

    # Rename some columns for consistency
    pre_race_df_renamed = pre_race_df.rename(columns={'end_position': 'position', 'round_number': 'round'})

    # Combine all historical races
    all_races = pd.concat([races_2001[['driver_id', 'driver_name', 'year', 'round', 'position', 'position_status', 'points', 'team_id']], 
                        pre_race_df_renamed[['driver_id', 'driver_name', 'year', 'round', 'position', 'position_status', 'points', 'team_id']]], 
                        ignore_index=True)

    # Sort chronologically by year and round for each driver
    all_races = all_races.sort_values(['driver_id', 'year', 'round'])

    # Calculate cumulative stats BEFORE each race
    all_races['cumulative_races'] = (
        all_races.groupby('driver_id').cumcount().shift(1).fillna(0)
    )
    all_races['cumulative_wins'] = (
        all_races.groupby('driver_id')['position']
        .transform(lambda x: (x == 1).cumsum().shift(1).fillna(0))
    )
    all_races['cumulative_podiums'] = (
        all_races.groupby('driver_id')['position']
        .transform(lambda x: x.isin([1, 2, 3]).cumsum().shift(1).fillna(0))
    )
    all_races['cumulative_points'] = (
        all_races.groupby('driver_id')['points']
        .transform(lambda x: x.cumsum().shift(1).fillna(0))
    )

    # Create rolling average finish columns, keep NA until enough races are completed
    for w in windows:
        all_races[f'avg_finish_last_{w}'] = (
            all_races.groupby('driver_id')['position']
            .transform(lambda x: x.shift(1).rolling(w, min_periods=w).mean())
        )

    # Calculate team average position from both drivers for imputing
    team_race_avg = all_races.groupby(['team_id', 'year', 'round'])['position'].mean().reset_index()
    team_race_avg = team_race_avg.rename(columns={'position': 'team_race_avg_position'})
    all_races = all_races.merge(team_race_avg, on=['team_id', 'year', 'round'], how='left')

    # Calculate rolling team averages
    for w in windows:
        all_races[f'team_avg_finish_last_{w}'] = (
            all_races.groupby('team_id')['team_race_avg_position']
            .transform(lambda x: x.shift(1).rolling(w, min_periods=1).mean())
        )

    # Filter to only include pre_qual_df races and merge the calculated features
    pre_race_enhanced = pre_race_df_renamed.merge(
        all_races[all_races['year'].isin(pre_race_df_renamed['year'].unique())][
            ['driver_id', 'year', 'round', 'cumulative_races', 'cumulative_wins', 'cumulative_podiums', 
            'cumulative_points', 'avg_finish_last_3', 'avg_finish_last_5', 'avg_finish_last_10',
            'team_avg_finish_last_3', 'team_avg_finish_last_5', 'team_avg_finish_last_10']
        ],
        on=['driver_id', 'year', 'round'],
        how='left'
    )

    # Fill driver NA values with team averages
    for w in windows:
        driver_col = f'avg_finish_last_{w}'
        team_col = f'team_avg_finish_last_{w}'
        pre_race_enhanced[driver_col] = pre_race_enhanced[driver_col].fillna(pre_race_enhanced[team_col])

    # Remove team average columns from final dataframe
    team_avg_cols = [f'team_avg_finish_last_{w}' for w in windows]
    pre_race_enhanced = pre_race_enhanced.drop(columns=team_avg_cols)

    # Add rookie flag
    first_seasons = all_races.groupby('driver_id')['year'].min().reset_index()
    first_seasons.columns = ['driver_id', 'first_season_year']

    # Merge with enhanced dataframe
    pre_race_enhanced = pre_race_enhanced.merge(first_seasons, on='driver_id', how='left')

    # Create rookie flag
    pre_race_enhanced['rookie_flag'] = pre_race_enhanced['year'] == pre_race_enhanced['first_season_year']

    # Drop the temporary column
    pre_race_enhanced = pre_race_enhanced.drop(columns=['first_season_year'])

    # Calculate rates for CLAS, DNF, and NC using all races
    for status in ['CLAS', 'DNF', 'NC']:
        overall_rate = (all_races['position_status'] == status).mean()
        
        all_races[f'{status}_rate'] = (
            all_races.groupby('driver_id')['position_status']
            .apply(lambda x: (x.shift(1) == status).expanding().mean())
            .fillna(overall_rate)
            .values
        )

    # Merge the calculated rates back to pre_race_enhanced
    pre_race_enhanced = pre_race_enhanced.merge(
        all_races[['driver_id', 'year', 'round', 'CLAS_rate', 'DNF_rate', 'NC_rate']],
        on=['driver_id', 'year', 'round'],
        how='left',
    )

    # Impute missing practices
    for session_num in [1, 2, 3]:
        best_time_col = f'best_time_FP{session_num}'
        recorded_time_col = f'recorded_lap_time_FP{session_num}'
        lap_count_col = f'lap_count_FP{session_num}'
        participated_col = f'participated_FP{session_num}'
        position_col = f'position_FP{session_num}'
        
        # Impute False for participated_FP* if lap_count_FP* is NA
        pre_race_enhanced.loc[pre_race_enhanced[lap_count_col].isna(), participated_col] = pre_race_enhanced.loc[pre_race_enhanced[lap_count_col].isna(), participated_col].fillna(False)

        # Impute 0 for NA rows in lap_count_FP*
        pre_race_enhanced[lap_count_col] = pre_race_enhanced[lap_count_col].fillna(0)
        
        # Impute False in recorded_lap_time_FP* if best_time_FP* is NA
        pre_race_enhanced.loc[pre_race_enhanced[best_time_col].isna(), recorded_time_col] = False
        

        for race_id, group in pre_race_enhanced.groupby('race_id'):
            
            # Impute last place for position_FP*
            if group[position_col].isna().any():
                max_position = group[position_col].max()
                if pd.notna(max_position):
                    missing_indices = group[group[position_col].isna()].index
                    pre_race_enhanced.loc[missing_indices, position_col] = max_position + 1
            
            # Impute best_time_FP*
            if group[best_time_col].isna().any():
                most_recent_best_time = group[best_time_col].dropna().iloc[-1] if not group[best_time_col].dropna().empty else None
                if most_recent_best_time is not None:
                    imputed_best_time = most_recent_best_time * 1.05
                    missing_indices = group[group[best_time_col].isna()].index
                    pre_race_enhanced.loc[missing_indices, best_time_col] = imputed_best_time
    
    # Impute missing pits
    pit_time_cols = ['avg_pit_time_last_5', 'avg_pit_time_last_10']
    for col in pit_time_cols:
        
        # Fill with team average for that race (teammate)
        team_race_avg = pre_race_enhanced.groupby(['team_id', 'race_id'])[col].transform('mean')
        pre_race_enhanced[col] = pre_race_enhanced[col].fillna(team_race_avg)
        
        # Fill with teams historical average
        team_historical_avg = pre_race_enhanced.groupby('team_id')[col].transform('mean')
        pre_race_enhanced[col] = pre_race_enhanced[col].fillna(team_historical_avg)
        
        # Fill with race average
        race_avg = pre_race_enhanced.groupby('race_id')[col].transform('mean')
        pre_race_enhanced[col] = pre_race_enhanced[col].fillna(race_avg)
        
        # Fill with overall column mean
        pre_race_enhanced[col] = pre_race_enhanced[col].fillna(pre_race_enhanced[col].mean())

    # Impute False for lap time flag when corresponding time is NA
    for session in (1, 2, 3):
        q_col = f'q{session}_time'
        flag_col = f'q{session}_no_lap_time_flag'
        
        # Create a mask for rows where q*_time is NA
        mask = pre_race_enhanced[q_col].isna()
        
        # Fill positions with False
        pre_race_enhanced[flag_col] = pre_race_enhanced[flag_col].where(~mask, False)

    # Impute qual_position and q*_time
    for race_id, group in pre_race_enhanced.groupby('race_id'):
        
        # Impute qual_position
        if group['qual_position'].isna().any():
            max_qual_position = group['qual_position'].max()
            if pd.notna(max_qual_position):
                missing_indices = group[group['qual_position'].isna()].index
                pre_race_enhanced.loc[missing_indices, 'qual_position'] = max_qual_position + 1
        
        # Impute q*_time for each session using the structure from cell 45-57
        for session in (1, 2, 3):
            q_time_col = f'q{session}_time'
            
            if group[q_time_col].isna().any():
                # Calculate the maximum valid q*_time for this race group
                max_time = group[q_time_col].max()
                
                if pd.notna(max_time):
                    if session == 1:
                        # Q1 imputation: +5s penalty
                        pre_race_enhanced.loc[group[group[q_time_col].isna()].index, q_time_col] = max_time + 5
                    elif session == 2:
                        # Q2 imputation: +10s penalty (no advanced_to_q2 column available)
                        pre_race_enhanced.loc[group[group[q_time_col].isna()].index, q_time_col] = max_time + 10
                    elif session == 3:
                        # Q3 imputation: +10s penalty (no advanced_to_q3 column available)
                        pre_race_enhanced.loc[group[group[q_time_col].isna()].index, q_time_col] = max_time + 10

    # Impute start_position for rows where it's NA
    for race_id, group in pre_race_enhanced.groupby('race_id'):
        if group['start_position'].isna().any():
            # Get the maximum start_position in this race
            max_start_position = group['start_position'].max()
            if pd.notna(max_start_position):
                # Get all rows with missing start_position in this race
                missing_indices = group[group['start_position'].isna()].index
                
                # Assign start_positions sequentially starting from max + 1
                for i, idx in enumerate(missing_indices, start=1):
                    pre_race_enhanced.loc[idx, 'start_position'] = max_start_position + i
            else:
                # If all start_positions are missing in this race, assign sequential positions starting from 1
                missing_indices = group[group['start_position'].isna()].index
                for i, idx in enumerate(missing_indices, start=1):
                    pre_race_enhanced.loc[idx, 'start_position'] = i
    
    # Sort columns
    pre_race_clean = pre_race_enhanced[[
        'race_id',
        'driver_id',
        'circuit_id',
        'team_id',
        'year',
        'round',
        'driver_name',
        'team_name',
        'cumulative_races',
        'cumulative_wins',
        'cumulative_podiums',
        'cumulative_points',
        'avg_finish_last_3',
        'avg_finish_last_5',
        'avg_finish_last_10',
        'position',  # Target
        'start_position',
        'points',  # Remove later
        'laps_completed',  # Remove later
        'CLAS_rate',
        'NC_rate',
        'DNF_rate',
        'rookie_flag',
        'circuit_name_x',  # Rename
        'type',
        'direction',
        'length',
        'turns',
        'elevation',    
        'yellow_flag_prob',
        'double_yellow_prob',
        'red_flag_prob',
        'safety_car_prob',
        'vsc_prob',
        'avg_yellow_count',
        'avg_double_yellow_count',
        'avg_red_count',
        'avg_safety_car_deployments',
        'avg_vsc_deployments',
        'avg_sc_laps',
        'avg_vsc_laps',
        'best_time_FP1',
        'best_time_FP2',
        'best_time_FP3',
        'lap_count_FP1',
        'lap_count_FP2',
        'lap_count_FP3',
        'position_FP1',
        'position_FP2',
        'position_FP3',
        'recorded_lap_time_FP1',
        'recorded_lap_time_FP2',
        'recorded_lap_time_FP3',
        'participated_FP1',
        'participated_FP2',
        'participated_FP3',
        'q1_time',
        'q2_time',
        'q3_time',
        'qual_position',
        'q1_no_lap_time_flag',
        'q2_no_lap_time_flag',
        'q3_no_lap_time_flag',
        'avg_pit_time_last_5',
        'avg_pit_time_last_10',
        'AirTemp_mean',
        'AirTemp_min',
        'AirTemp_max',
        'AirTemp_std',
        'TrackTemp_mean',
        'TrackTemp_min',
        'TrackTemp_max',
        'TrackTemp_std',
        'WindSpeed_mean',
        'WindSpeed_min',
        'WindSpeed_max',
        'WindSpeed_std',
        'Humidity_mean',
        'Humidity_min',
        'Humidity_max',
        'Humidity_std',
        'Pressure_mean',
        'Pressure_min',
        'Pressure_max',
        'Pressure_std',
        'Rainfall_any',
        'Rainfall_mean',
        'used_soft_FP1',
        'avg_pace_soft_FP1',
        'std_pace_soft_FP1',
        'deg_rate_soft_FP1',
        'used_soft_FP2',
        'avg_pace_soft_FP2',
        'std_pace_soft_FP2',
        'deg_rate_soft_FP2',
        'used_soft_FP3',
        'avg_pace_soft_FP3',
        'std_pace_soft_FP3',
        'deg_rate_soft_FP3',
        'used_soft_Qualifying',
        'avg_pace_soft_Qualifying',
        'std_pace_soft_Qualifying',
        'deg_rate_soft_Qualifying',
        'used_medium_FP1',
        'avg_pace_medium_FP1',
        'std_pace_medium_FP1',
        'deg_rate_medium_FP1',
        'used_medium_FP2',
        'avg_pace_medium_FP2',
        'std_pace_medium_FP2',    
        'deg_rate_medium_FP2',
        'used_medium_FP3',
        'avg_pace_medium_FP3',
        'std_pace_medium_FP3',
        'deg_rate_medium_FP3',
        'used_medium_Qualifying',
        'avg_pace_medium_Qualifying',
        'std_pace_medium_Qualifying',
        'deg_rate_medium_Qualifying',
        'used_hard_FP1',
        'avg_pace_hard_FP1',
        'std_pace_hard_FP1',
        'deg_rate_hard_FP1',
        'used_hard_FP2',
        'avg_pace_hard_FP2',
        'std_pace_hard_FP2',
        'deg_rate_hard_FP2',
        'used_hard_FP3',
        'avg_pace_hard_FP3',
        'std_pace_hard_FP3',
        'deg_rate_hard_FP3',
        'used_hard_Qualifying',
        'avg_pace_hard_Qualifying',
        'std_pace_hard_Qualifying',
        'deg_rate_hard_Qualifying',
        'used_intermediate_FP1',
        'avg_pace_intermediate_FP1',
        'std_pace_intermediate_FP1',
        'deg_rate_intermediate_FP1',
        'used_intermediate_FP2',
        'avg_pace_intermediate_FP2',
        'std_pace_intermediate_FP2',
        'deg_rate_intermediate_FP2',
        'used_intermediate_FP3',
        'avg_pace_intermediate_FP3',   
        'std_pace_intermediate_FP3',
        'deg_rate_intermediate_FP3',
        'used_intermediate_Qualifying',
        'avg_pace_intermediate_Qualifying',
        'std_pace_intermediate_Qualifying',
        'deg_rate_intermediate_Qualifying',
        'used_wet_FP1',
        'avg_pace_wet_FP1',
        'std_pace_wet_FP1',
        'deg_rate_wet_FP1',
        'used_wet_FP2',
        'avg_pace_wet_FP2',
        'std_pace_wet_FP2',
        'deg_rate_wet_FP2',
        'used_wet_FP3',
        'avg_pace_wet_FP3',
        'std_pace_wet_FP3',
        'deg_rate_wet_FP3',
        'used_wet_Qualifying',
        'avg_pace_wet_Qualifying',
        'std_pace_wet_Qualifying',
        'deg_rate_wet_Qualifying'
    ]].rename(columns={'circuit_name_x': 'circuit_name'})

    # Correct datatypes
    pre_race_clean['cumulative_races'] = pre_race_clean['cumulative_races'].astype(int)
    pre_race_clean['cumulative_wins'] = pre_race_clean['cumulative_wins'].astype(int)
    pre_race_clean['cumulative_podiums'] = pre_race_clean['cumulative_podiums'].astype(int)
    pre_race_clean['start_position'] = pre_race_clean['start_position'].astype(int)
    pre_race_clean['laps_completed'] = pre_race_clean['laps_completed'].astype(int)
    pre_race_clean['elevation'] = pre_race_clean['elevation'].astype(int)

    for session in range(1, 4):
        pre_race_clean[f'lap_count_FP{session}'] = pre_race_clean[f'lap_count_FP{session}'].astype(int)
        pre_race_clean[f'position_FP{session}'] = pre_race_clean[f'position_FP{session}'].astype(int)
        pre_race_clean[f'recorded_lap_time_FP{session}'] = pre_race_clean[f'recorded_lap_time_FP{session}'].astype(bool)
        pre_race_clean[f'participated_FP{session}'] = pre_race_clean[f'participated_FP{session}'].astype(bool)
        pre_race_clean[f'q{session}_no_lap_time_flag'] = pre_race_clean[f'q{session}_no_lap_time_flag'].astype(bool)

    pre_race_clean['qual_position'] = pre_race_clean['qual_position'].astype(int)

    # Save
    pre_race_shape = pre_race_clean.shape
    print(f"   Shape: {pre_race_shape}")
    pre_race_clean.to_csv(os.path.join(FINAL_FOLDER_PATH, 'f1_data_pre_race_clean.csv'), encoding='utf-8', index=False)
    print("   Pre-race data cleaned")