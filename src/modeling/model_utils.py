# Jack Wilson
# 2/11/2026
# Utility functions for modeling pipeline

# --------------------------------------------------------------------------------
# Import modules

import pandas as pd
import numpy as np
import os, sys
from catboost import CatBoostClassifier, Pool
from sklearn.model_selection import GroupShuffleSplit
import warnings

warnings.filterwarnings('ignore')

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# --------------------------------------------------------------------------------
# Data Loading and Preparation

def load_prediction_input(from_file=True, grand_prix=None):
    """
    Load drivers, teams, and grand prix for predictions.
    
    Parameters:
    -----------
    from_file : bool
        If True, read from pred_list.xlsx. If False, use most recent race.
    grand_prix : str, optional
        Required if from_file=False. Grand prix name to predict.
    
    Returns:
    --------
    drivers : list
        List of driver names
    teams : list
        List of team names (corresponding to drivers)
    grand_prix : str
        Grand prix name
    """
    if from_file:
        # Read from pred_list.xlsx
        pred_list_path = os.path.join(PROJECT_ROOT, "src", "modeling", "driver_pred_list.xlsx")
        if not os.path.exists(pred_list_path):
            raise FileNotFoundError(f"driver_pred_list.xlsx not found at {pred_list_path}")
        
        df = pd.read_excel(pred_list_path, sheet_name='list')
        # Filter out rows with missing driver names
        df = df[df['name'].notna()]
        drivers = df['name'].tolist()
        teams = df['team'].tolist()
        
        # Get grand prix from first non-null entry
        grand_prix = df['grand_prix'].dropna().iloc[0] if df['grand_prix'].notna().any() else None
        if grand_prix is None:
            raise ValueError("No grand prix specified in pred_list.xlsx")
        
        return drivers, teams, grand_prix
    else:
        # Use most recent race
        if grand_prix is None:
            raise ValueError("grand_prix must be specified when from_file=False")
        
        # Load data to get most recent race
        data_path = os.path.join(PROJECT_ROOT, "data", "final", "f1_data_pre_race_clean.csv")
        df = pd.read_csv(data_path, low_memory=False)
        
        # Get most recent race (highest year and round)
        df_sorted = df.sort_values(['year', 'round'], ascending=False)
        most_recent_year = df_sorted.iloc[0]['year']
        most_recent_round = df_sorted.iloc[0]['round']
        most_recent = df[(df['year'] == most_recent_year) & (df['round'] == most_recent_round)]
        
        # Get unique driver-team pairs, preserving order
        driver_team_map = most_recent.groupby('driver_name')['team_name'].first().to_dict()
        drivers = list(driver_team_map.keys())
        teams = [driver_team_map[driver] for driver in drivers]
        
        return drivers, teams, grand_prix


def prepare_features(df, data_type='pre_qual', one_hot_encode=True):
    """
    Prepare features by removing targets and data leakage variables.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Input dataframe
    data_type : str
        'pre_qual' or 'pre_race'
    one_hot_encode : bool
        If True, one-hot encode categorical variables (type, direction)
    
    Returns:
    --------
    df_features : pd.DataFrame
        DataFrame with features only
    feature_cols : list
        List of feature column names
    """
    df_features = df.copy()
    
    # Always remove target and identifiers
    exclude_cols = [
        'position',  # Target
        'points',  # Data leakage
        'laps_completed',  # Data leakage
        'year',  # Identifier
        'round',  # Identifier
        'driver_name',  # Identifier
        'team_name',  # Identifier
        'circuit_name',  # Identifier
    ]
    
    # Pre-race also has start_position as leakage
    if data_type == 'pre_race':
        exclude_cols.append('start_position')
    
    # Remove columns that exist
    exclude_cols = [col for col in exclude_cols if col in df_features.columns]
    df_features = df_features.drop(columns=exclude_cols)
    
    # One-hot encode categorical variables if needed
    if one_hot_encode:
        if 'type' in df_features.columns:
            df_features = pd.get_dummies(df_features, columns=['type'], prefix='type')
        if 'direction' in df_features.columns:
            df_features = pd.get_dummies(df_features, columns=['direction'], prefix='direction')
    
    # Convert boolean columns to integers
    bool_cols = df_features.select_dtypes(include='bool').columns
    for col in bool_cols:
        df_features[col] = df_features[col].astype(int)
    
    # Get feature columns
    feature_cols = list(df_features.columns)
    
    return df_features, feature_cols


def apply_time_weighting(df, year_col='year', round_col='round'):
    """
    Apply time-based weighting to training data.
    More recent races (higher year and round) get higher weights.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Training dataframe with year and round columns
    year_col : str
        Name of year column
    round_col : str
        Name of round column
    
    Returns:
    --------
    weights : np.array
        Array of sample weights
    """
    # Normalize year and round to 0-1 scale
    max_year = df[year_col].max()
    min_year = df[year_col].min()
    max_round = df[round_col].max()
    min_round = df[round_col].min()
    
    # Normalize
    year_norm = (df[year_col] - min_year) / (max_year - min_year + 1e-10)
    round_norm = (df[round_col] - min_round) / (max_round - min_round + 1e-10)
    
    # Combine: more recent = higher weight
    # Weight = (year_weight * 0.7 + round_weight * 0.3) * 2 + 0.5
    # This gives weights roughly in range 0.5 to 2.5
    weights = (year_norm * 0.7 + round_norm * 0.3) * 2 + 0.5
    
    return weights.values


def train_model(X_train, y_train, X_val, y_val, sample_weights=None, 
                groups=None, n_classes=20, iterations=1000, learning_rate=0.05,
                depth=6, l2_leaf_reg=3, verbose=False):
    """
    Train a CatBoost multiclass classifier.
    
    Parameters:
    -----------
    X_train : pd.DataFrame
        Training features
    y_train : pd.Series
        Training targets (0 to n_classes-1)
    X_val : pd.DataFrame
        Validation features
    y_val : pd.Series
        Validation targets
    sample_weights : np.array, optional
        Sample weights for training
    groups : pd.Series, optional
        Group labels for GroupShuffleSplit
    n_classes : int
        Number of classes
    iterations : int
        Number of boosting iterations
    learning_rate : float
        Learning rate
    depth : int
        Tree depth
    l2_leaf_reg : float
        L2 regularization
    verbose : bool
        Whether to print training progress
    
    Returns:
    --------
    model : CatBoostClassifier
        Trained model
    """
    # Create Pool objects
    train_pool = Pool(
        data=X_train,
        label=y_train,
        weight=sample_weights
    )
    
    val_pool = Pool(
        data=X_val,
        label=y_val
    )
    
    # Train model
    model = CatBoostClassifier(
        iterations=iterations,
        learning_rate=learning_rate,
        depth=depth,
        l2_leaf_reg=l2_leaf_reg,
        loss_function='MultiClass',
        eval_metric='MultiClass',
        random_seed=42,
        verbose=verbose,
        early_stopping_rounds=50,
        use_best_model=True
    )
    
    model.fit(
        train_pool,
        eval_set=val_pool,
        verbose=verbose
    )
    
    return model


def format_predictions(proba, drivers, teams, grand_prix):
    """
    Format predictions into individual driver tables and aggregate race outcome.
    
    Parameters:
    -----------
    proba : np.array
        Probability matrix (n_drivers, n_positions)
    drivers : list
        List of driver names
    teams : list
        List of team names
    grand_prix : str
        Grand prix name
    
    Returns:
    --------
    driver_tables : dict
        Dictionary with driver names as keys and DataFrames as values
    race_outcome : pd.DataFrame
        DataFrame with predicted race outcome (P1, P2, ..., Pn)
    """
    n_drivers, n_positions = proba.shape
    
    # Ensure we don't have more positions than drivers
    n_positions = min(n_positions, n_drivers)
    
    # Create individual driver tables
    driver_tables = {}
    for i, driver in enumerate(drivers):
        driver_df = pd.DataFrame({
            'Position': [f'P{j+1}' for j in range(n_positions)],
            'Probability': proba[i, :n_positions],
            'Percentage': proba[i, :n_positions] * 100
        })
        driver_df['Percentage'] = driver_df['Percentage'].round(2)
        driver_tables[driver] = driver_df
    
    # Create aggregate race outcome
    # Use a greedy assignment: for each position, assign the driver with highest probability
    # that hasn't been assigned yet
    race_outcome = []
    used_drivers = set()
    
    for pos in range(n_positions):
        # Find driver with highest probability for this position (excluding already used)
        best_idx = None
        best_prob = -1
        
        for i, driver in enumerate(drivers):
            if driver not in used_drivers:
                if proba[i, pos] > best_prob:
                    best_prob = proba[i, pos]
                    best_idx = i
        
        if best_idx is not None:
            driver = drivers[best_idx]
            team = teams[best_idx]
            race_outcome.append({
                'Position': f'P{pos+1}',
                'Driver': driver,
                'Team': team,
                'Probability': proba[best_idx, pos],
                'Percentage': proba[best_idx, pos] * 100
            })
            used_drivers.add(driver)
        else:
            # If no driver found (shouldn't happen), break
            break
    
    race_outcome_df = pd.DataFrame(race_outcome)
    if len(race_outcome_df) > 0:
        race_outcome_df['Percentage'] = race_outcome_df['Percentage'].round(2)

    return driver_tables, race_outcome_df


# Maximum grid size (current F1 rules)
MAX_POSITIONS = 22


def driver_percentages_dataframe(driver_tables, drivers, teams):
    """
    Build a single DataFrame with one row per driver and columns Driver, Team, P1, P2, ... Pn.

    Parameters:
    -----------
    driver_tables : dict
        Dict of driver name -> DataFrame with Position and Percentage columns
    drivers : list
        List of driver names (order preserved)
    teams : list
        List of team names

    Returns:
    --------
    pd.DataFrame
        Columns: Driver, Team, P1, P2, ... Pn
    """
    rows = []
    for i, driver in enumerate(drivers):
        if driver not in driver_tables:
            continue
        df = driver_tables[driver]
        pct = df.set_index('Position')['Percentage']
        row = {'Driver': driver, 'Team': teams[i]}
        for pos in df['Position'].tolist():
            row[pos] = pct.get(pos, 0)
        rows.append(row)
    return pd.DataFrame(rows)
