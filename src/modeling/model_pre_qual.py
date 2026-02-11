# Jack Wilson
# 2/11/2026
# Model for pre-qualifying predictions (practice data only)

# --------------------------------------------------------------------------------
# Import modules

import pandas as pd
import numpy as np
import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.modeling.model_utils import (
    load_prediction_input,
    prepare_features,
    apply_time_weighting,
    train_model,
    format_predictions,
    MAX_POSITIONS,
)
from sklearn.model_selection import GroupShuffleSplit
from catboost import CatBoostClassifier, Pool
import warnings

warnings.filterwarnings('ignore')


# --------------------------------------------------------------------------------

def model_pre_qual(from_file=True, grand_prix=None, train_new_model=True, 
                   model_path=None, iterations=1000, learning_rate=0.05,
                   depth=6, l2_leaf_reg=3, verbose=False):
    """
    Train and/or use model for pre-qualifying predictions.
    
    Parameters:
    -----------
    from_file : bool
        If True, read drivers/teams/grand_prix from pred_list.xlsx
    grand_prix : str, optional
        Required if from_file=False. Grand prix name to predict.
    train_new_model : bool
        If True, train a new model. If False, load existing model.
    model_path : str, optional
        Path to save/load model. Defaults to models/model_pre_qual.cbm
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
    driver_tables : dict
        Dictionary with driver names as keys and probability DataFrames as values
    race_outcome : pd.DataFrame
        DataFrame with predicted race outcome
    """
    # Set default model path
    if model_path is None:
        models_dir = os.path.join(PROJECT_ROOT, "src", "modeling")
        os.makedirs(models_dir, exist_ok=True)
        model_path = os.path.join(models_dir, "model_pre_qual.cbm")
    
    # Load prediction input
    drivers, teams, grand_prix = load_prediction_input(from_file, grand_prix)
    print(f"   Predicting for {len(drivers)} drivers at {grand_prix}...")
    
    # Load training data
    data_path = os.path.join(PROJECT_ROOT, "data", "final", "f1_data_pre_qual_clean.csv")
    df = pd.read_csv(data_path, low_memory=False)
    
    # Prepare features
    df_features, feature_cols = prepare_features(df, data_type='pre_qual')
    
    # Prepare target (map 1-22 to 0-21; clip so historical 1-20 stays 0-19)
    y = (df['position'] - 1).clip(0, MAX_POSITIONS - 1)
    
    # Apply time weighting
    sample_weights = apply_time_weighting(df)
    
    # Split data
    gss = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
    train_idx, val_idx = next(gss.split(df_features, y, groups=df['race_id']))
    
    X_train = df_features.iloc[train_idx]
    X_val = df_features.iloc[val_idx]
    y_train = y.iloc[train_idx]
    y_val = y.iloc[val_idx]
    weights_train = sample_weights[train_idx]
    
    # Train or load model
    if train_new_model:
        print("   Training pre-qualifying model...")
        model = train_model(
            X_train, y_train, X_val, y_val,
            sample_weights=weights_train,
            n_classes=MAX_POSITIONS,
            iterations=iterations,
            learning_rate=learning_rate,
            depth=depth,
            l2_leaf_reg=l2_leaf_reg,
            verbose=verbose
        )
        # Save model
        os.makedirs(os.path.dirname(model_path), exist_ok=True)
        model.save_model(model_path)
        print("   Model saved")
    else:
        print("   Loading existing model...")
        model = CatBoostClassifier()
        model.load_model(model_path)
    
    # Prepare prediction data
    # Try to find data for the specified grand prix
    pred_data = df[df['circuit_name'] == grand_prix].copy()
    
    # If no data for this grand prix, use most recent race data
    if len(pred_data) == 0:
        print(f"   Warning: No historical data found for {grand_prix}. Using most recent race data...")
        df_sorted = df.sort_values(['year', 'round'], ascending=False)
        most_recent_year = df_sorted.iloc[0]['year']
        most_recent_round = df_sorted.iloc[0]['round']
        pred_data = df[(df['year'] == most_recent_year) & (df['round'] == most_recent_round)].copy()
        
        # Update circuit_name to match requested grand prix
        pred_data['circuit_name'] = grand_prix
    
    # Filter to requested drivers
    pred_data = pred_data[pred_data['driver_name'].isin(drivers)].copy()
    
    # Ensure we have data for all requested drivers
    if len(pred_data) < len(drivers):
        missing = set(drivers) - set(pred_data['driver_name'].unique())
        print(f"   Warning: Missing data for drivers: {missing}")
        
        # Use most recent race data for missing drivers
        df_sorted = df.sort_values(['year', 'round'], ascending=False)
        most_recent_year = df_sorted.iloc[0]['year']
        most_recent_round = df_sorted.iloc[0]['round']
        most_recent = df[(df['year'] == most_recent_year) & (df['round'] == most_recent_round)]
        for driver in missing:
            driver_data = most_recent[most_recent['driver_name'] == driver].iloc[0:1].copy()
            if len(driver_data) > 0:
                driver_data['circuit_name'] = grand_prix
                pred_data = pd.concat([pred_data, driver_data], ignore_index=True)
    
    # Ensure correct order
    driver_order = []
    team_order = []
    for driver, team in zip(drivers, teams):
        if driver in pred_data['driver_name'].values:
            driver_order.append(driver)
            team_order.append(team)
    
    if len(driver_order) == 0:
        raise ValueError(f"   No data found for any of the requested drivers: {drivers}")
    
    pred_data = pred_data[pred_data['driver_name'].isin(driver_order)].copy()
    pred_data = pred_data.set_index('driver_name').loc[driver_order].reset_index()
    
    # Prepare features for prediction
    X_pred, _ = prepare_features(pred_data, data_type='pre_qual')
    
    # Ensure feature columns match training
    missing_cols = set(feature_cols) - set(X_pred.columns)
    if missing_cols:
        for col in missing_cols:
            X_pred[col] = 0
    
    extra_cols = set(X_pred.columns) - set(feature_cols)
    if extra_cols:
        X_pred = X_pred.drop(columns=extra_cols)
    
    X_pred = X_pred[feature_cols]
    
    # Predict probabilities: (n_drivers, n_classes). Support variable grid size.
    n_drivers = len(driver_order)
    proba = model.predict_proba(X_pred)
    n_classes = proba.shape[1]
    if n_drivers > n_classes:
        # Pad with zero columns and renormalize so we have P1..Pn_drivers
        pad = np.zeros((n_drivers, n_drivers - n_classes))
        proba = np.hstack([proba, pad])
        proba = proba / proba.sum(axis=1, keepdims=True)
    else:
        proba = proba[:, :n_drivers]
    proba = np.asarray(proba, dtype=float)

    # Format predictions
    driver_tables, race_outcome = format_predictions(
        proba, driver_order, team_order, grand_prix
    )
    
    return driver_tables, race_outcome
