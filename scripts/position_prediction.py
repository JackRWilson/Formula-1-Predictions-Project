# Jack Wilson
# 12/2/2025
# This file is a pipeline used to 

"""
F1 Race Position Prediction with Full Probability Distribution
================================================================
This script trains a multiclass classifier to predict finishing positions (1-20)
and outputs complete probability distributions for each driver.
"""

# ==============================================================================================
# Import Modules & Settings
# ==============================================================================================

import pandas as pd
import numpy as np
import os
from sklearn.model_selection import GroupShuffleSplit
from sklearn.metrics import (
    accuracy_score, log_loss, top_k_accuracy_score
)
from scipy.stats import kendalltau
from catboost import CatBoostClassifier, Pool
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings('ignore')


# ==============================================================================================
# Inputs
# ==============================================================================================

# Data path from the project root folder
data_path = 'data/final/f1_data_pre_qual_final.csv'

# Columns in the data to exclude from features (target, ids, etc.)
exclude_cols = ['position', 'driver_id', 'team_id', 'race_id']

# ==============================================================================================
# I. Data Preparation & Splitting
# ==============================================================================================

print("=" * 70)
print("I: Loading and Splitting Data")
print("=" * 70)

# Load dataframe
current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(current_dir)
file_path = os.path.join(PROJECT_ROOT, data_path)
if os.path.exists(file_path):
    df = pd.read_csv(file_path)
else:
    print(f'Error: File not found at {file_path}')

# Define features
feature_cols = [col for col in df.columns if col not in exclude_cols]

print(f"Total samples: {len(df)}")
print(f"Number of features: {len(feature_cols)}")
print(f"Position distribution:\n{df['position'].value_counts().sort_index()}")

# Split data using GroupShuffleSplit to keep races together
# This prevents data leakage where drivers from same race appear in both train/val
gss = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
train_idx, val_idx = next(gss.split(df, groups=df['race_id']))

train_df = df.iloc[train_idx].copy()
val_df = df.iloc[val_idx].copy()

print(f"\nTrain samples: {len(train_df)}")
print(f"Validation samples: {len(val_df)}")
print(f"Train races: {train_df['race_id'].nunique()}")
print(f"Validation races: {val_df['race_id'].nunique()}")

# Prepare feature matrices and target vectors
X_train = train_df[feature_cols]
X_val = val_df[feature_cols]

# Map positions from 1-20 to 0-19 for model training (required for class indices)
y_train = train_df['position'] - 1  # Now 0-19
y_val = val_df['position'] - 1      # Now 0-19

print(f"\nTarget range after mapping: {y_train.min()} to {y_train.max()}")

# ==============================================================================================
# II. Check Class Imblance and Calculate Weights
# ==============================================================================================

print("\n" + "=" * 70)
print("II. Analyzing Class Imbalance")
print("=" * 70)

# Calculate class distribution
class_counts = y_train.value_counts().sort_index()
total_samples = len(y_train)
n_classes = 20

# Calculate inverse frequency weights for handling class imbalance
# Weight = total_samples / (n_classes * count_for_class)
class_weights = {}
for class_idx in range(n_classes):
    count = class_counts.get(class_idx, 1)  # Avoid division by zero
    class_weights[class_idx] = total_samples / (n_classes * count)

print("Class weights (first 5 positions):")
for i in range(5):
    print(f"  P{i+1} (class {i}): {class_weights[i]:.3f}")

# Convert to sample weights for CatBoost
sample_weights_train = np.array([class_weights[y] for y in y_train])

# ==============================================================================================
# III. Train CatBoost Model With Hyperparameter Tuning
# ==============================================================================================
# PERFORMANCE NOTE: Grid search can be slow (16 combinations × up to 1000 iterations each).
# Set ENABLE_GRID_SEARCH=False below to skip tuning and use default parameters for ~16x faster training.
# ==============================================================================================

print("\n" + "=" * 70)
print("III. Training CatBoost Classifier")
print("=" * 70)

# Create CatBoost Pool objects for efficient training
train_pool = Pool(
    data=X_train,
    label=y_train,
    weight=sample_weights_train
)

val_pool = Pool(
    data=X_val,
    label=y_val
)

# HYPERPARAMETER TUNING OPTIONS
# Set to False to skip grid search and use default parameters (much faster)
ENABLE_GRID_SEARCH = True  # Set to False for faster training

# Define parameter grid for tuning (reduced for faster execution)
# For faster runs, you can reduce iterations or use fewer combinations
param_grid = {
    'iterations': [500, 1000],
    'learning_rate': [0.03, 0.05],
    'depth': [6, 8],
    'l2_leaf_reg': [3, 5]
}

if ENABLE_GRID_SEARCH:
    best_score = float('inf')
    best_params = None
    best_model = None
    
    # Calculate total combinations for progress tracking
    total_combos = (len(param_grid['iterations']) * 
                   len(param_grid['learning_rate']) * 
                   len(param_grid['depth']) * 
                   len(param_grid['l2_leaf_reg']))
    
    print(f"Starting hyperparameter search ({total_combos} combinations)...")
    print("(This may take a while - consider setting ENABLE_GRID_SEARCH=False for faster runs)")
    
    combo_num = 0
    # Quick grid search (you can expand this for more thorough tuning)
    for iterations in param_grid['iterations']:
        for lr in param_grid['learning_rate']:
            for depth in param_grid['depth']:
                for l2_reg in param_grid['l2_leaf_reg']:
                    combo_num += 1
                    print(f"\n[{combo_num}/{total_combos}] Testing: iter={iterations}, lr={lr}, depth={depth}, l2={l2_reg}")
                    
                    model = CatBoostClassifier(
                        iterations=iterations,
                        learning_rate=lr,
                        depth=depth,
                        l2_leaf_reg=l2_reg,
                        loss_function='MultiClass',  # Multiclass classification
                        eval_metric='MultiClass',     # Evaluation metric
                        random_seed=42,
                        verbose=False,
                        early_stopping_rounds=50,
                        use_best_model=True
                    )
                    
                    # Train model
                    model.fit(
                        train_pool,
                        eval_set=val_pool,
                        verbose=False
                    )
                    
                    # Get validation score (lower is better for MultiClass loss)
                    val_score = model.get_best_score()['validation']['MultiClass']
                    print(f"  Validation score: {val_score:.4f}")
                    
                    if val_score < best_score:
                        best_score = val_score
                        best_params = {
                            'iterations': iterations,
                            'learning_rate': lr,
                            'depth': depth,
                            'l2_leaf_reg': l2_reg
                        }
                        best_model = model
                        print(f"  ✓ New best score!")
    
    print(f"\nBest validation score: {best_score:.4f}")
    print(f"Best parameters: {best_params}")
    
    # Reuse the best model from grid search instead of retraining
    print("\nUsing best model from grid search (no retraining needed)...")
    final_model = best_model
    
else:
    # Use default/reasonable parameters for faster training
    print("Skipping grid search - using default parameters for faster training...")
    best_params = {
        'iterations': 1000,
        'learning_rate': 0.05,
        'depth': 6,
        'l2_leaf_reg': 3
    }
    
    print(f"Training with parameters: {best_params}")
    final_model = CatBoostClassifier(
        **best_params,
        loss_function='MultiClass',
        eval_metric='MultiClass',
        random_seed=42,
        verbose=100,
        early_stopping_rounds=50,
        use_best_model=True
    )
    
    final_model.fit(
        train_pool,
        eval_set=val_pool
    )

print("\nModel training completed!")

# ==============================================================================================
# IV. Generate Probability Predictions
# ==============================================================================================

print("\n" + "=" * 70)
print("IV. Generating Probability Predictions")
print("=" * 70)

# Get probability predictions for validation set
# Shape: (n_samples, 20) where each row sums to 1.0
proba_val = final_model.predict_proba(X_val)

print(f"Probability matrix shape: {proba_val.shape}")
print(f"First sample probabilities sum: {proba_val[0].sum():.6f}")

# Create DataFrame with probability distributions
prob_df = pd.DataFrame(
    proba_val,
    columns=[f'P{i+1}_prob' for i in range(20)],
    index=val_df.index
)

# Add metadata columns
prob_df['race_id'] = val_df['race_id'].values
prob_df['driver_id'] = val_df['driver_id'].values
prob_df['actual_position'] = val_df['position'].values  # Original 1-20 scale

# Calculate expected position: sum(p_i * position_i)
expected_positions = np.sum(proba_val * np.arange(1, 21), axis=1)
prob_df['expected_position'] = expected_positions

# Calculate most likely position (argmax)
prob_df['predicted_position'] = np.argmax(proba_val, axis=1) + 1  # Map back to 1-20

# Reorder columns for better readability
first_cols = ['race_id', 'driver_id', 'actual_position', 
              'predicted_position', 'expected_position']
prob_cols = [f'P{i+1}_prob' for i in range(20)]
prob_df = prob_df[first_cols + prob_cols]

# Save to CSV
prob_df.to_csv('predicted_probabilities.csv', index=False)
print("\nProbability predictions saved to 'predicted_probabilities.csv'")

# Display sample predictions
print("\nSample predictions (first 3 drivers):")
print(prob_df.head(3).to_string())

# ============================================================================
# STEP 5: EVALUATE MODEL - CLASSIFICATION METRICS
# ============================================================================

print("\n" + "=" * 70)
print("STEP 5: Evaluation - Classification Metrics")
print("=" * 70)

y_pred = np.argmax(proba_val, axis=1)  # Predicted class (0-19)
y_true = y_val.values                   # Actual class (0-19)

# Accuracy@1: Exact position match
acc1 = accuracy_score(y_true, y_pred)
print(f"Accuracy@1 (exact position): {acc1:.4f} ({acc1*100:.2f}%)")

# Top-3 Accuracy: Driver finishes in predicted top-3
# For this, we check if actual position is in top-3 predicted positions
top3_acc = top_k_accuracy_score(y_true, proba_val, k=3)
print(f"Top-3 Accuracy: {top3_acc:.4f} ({top3_acc*100:.2f}%)")

# Top-5 Accuracy (bonus metric)
top5_acc = top_k_accuracy_score(y_true, proba_val, k=5)
print(f"Top-5 Accuracy: {top5_acc:.4f} ({top5_acc*100:.2f}%)")

# Log Loss: Measure of probability calibration (lower is better)
logloss = log_loss(y_true, proba_val)
print(f"Log Loss: {logloss:.4f}")

# Mean Absolute Error in predicted position
mae_position = np.mean(np.abs(y_true - y_pred))
print(f"MAE (positions off): {mae_position:.2f}")

# ============================================================================
# STEP 6: EVALUATE MODEL - RANKING METRICS
# ============================================================================

print("\n" + "=" * 70)
print("STEP 6: Evaluation - Ranking Metrics")
print("=" * 70)

def calculate_ndcg_at_k(y_true, y_score, k=20):
    """
    Calculate NDCG@k for ranking evaluation (vectorized for speed).
    y_true: actual positions (0-19)
    y_score: predicted probabilities or scores
    """
    # Vectorized implementation
    n_samples = len(y_true)
    
    # Get predicted rankings for all samples at once
    predicted_rankings = np.argsort(y_score, axis=1)[:, ::-1]  # Highest prob first
    
    # Find rank of actual position for each sample (vectorized)
    # Create index array to find positions
    ranks = np.zeros(n_samples, dtype=int)
    for i in range(n_samples):
        ranks[i] = np.where(predicted_rankings[i] == y_true[i])[0][0]
    
    # Calculate relevance (higher for better positions)
    relevance = 20 - y_true
    
    # Calculate DCG and IDCG (vectorized)
    dcg = relevance / np.log2(ranks + 2)
    idcg = relevance / np.log2(2)  # rank = 0 in ideal case
    
    # NDCG
    ndcg = np.where(idcg > 0, dcg / idcg, 0)
    
    return np.mean(ndcg)

ndcg = calculate_ndcg_at_k(y_true, proba_val, k=20)
print(f"NDCG@20: {ndcg:.4f}")

# Mean Reciprocal Rank (MRR)
# For each driver, find rank of actual position in predicted probabilities
def calculate_mrr(y_true, y_score):
    """Calculate Mean Reciprocal Rank (vectorized for speed)"""
    n_samples = len(y_true)
    
    # Get predicted rankings for all samples at once
    predicted_rankings = np.argsort(y_score, axis=1)[:, ::-1]
    
    # Find rank of actual position for each sample (vectorized)
    ranks = np.zeros(n_samples, dtype=int)
    for i in range(n_samples):
        ranks[i] = np.where(predicted_rankings[i] == y_true[i])[0][0] + 1
    
    # Calculate reciprocal ranks (vectorized)
    rr_scores = 1.0 / ranks
    
    return np.mean(rr_scores)

mrr = calculate_mrr(y_true, proba_val)
print(f"Mean Reciprocal Rank: {mrr:.4f}")

# Kendall's Tau: Race-level ranking correlation
# For each race, calculate correlation between predicted and actual rankings
def calculate_race_level_kendall(val_df, proba_val):
    """Calculate average Kendall's Tau across all races (optimized)"""
    tau_scores = []
    
    # Pre-calculate expected positions for all samples (vectorized)
    expected_positions = np.sum(proba_val * np.arange(1, 21), axis=1)
    
    # Group by race_id more efficiently
    for race_id in val_df['race_id'].unique():
        # Get all drivers in this race using boolean indexing
        race_mask = val_df['race_id'].values == race_id
        race_actual = val_df.loc[race_mask, 'position'].values
        race_predicted = expected_positions[race_mask]
        
        # Calculate Kendall's Tau
        if len(race_actual) > 1:  # Need at least 2 drivers
            tau, _ = kendalltau(race_actual, race_predicted)
            if not np.isnan(tau):
                tau_scores.append(tau)
    
    return np.mean(tau_scores) if tau_scores else 0.0

kendall_tau = calculate_race_level_kendall(val_df, proba_val)
print(f"Kendall's Tau (avg across races): {kendall_tau:.4f}")

# ============================================================================
# STEP 7: FEATURE IMPORTANCE
# ============================================================================

print("\n" + "=" * 70)
print("STEP 7: Feature Importance Analysis")
print("=" * 70)

# Get feature importances from CatBoost
feature_importance = final_model.get_feature_importance()
feature_names = X_train.columns

# Create DataFrame for easy viewing
importance_df = pd.DataFrame({
    'feature': feature_names,
    'importance': feature_importance
}).sort_values('importance', ascending=False)

print("\nTop 15 Most Important Features:")
print(importance_df.head(15).to_string(index=False))

# Plot feature importance
plt.figure(figsize=(10, 8))
top_n = 20
importance_df.head(top_n).plot(
    x='feature', 
    y='importance', 
    kind='barh', 
    figsize=(10, 8),
    legend=False
)
plt.xlabel('Importance')
plt.ylabel('Feature')
plt.title(f'Top {top_n} Most Important Features')
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig('feature_importance.png', dpi=300, bbox_inches='tight')
print("\nFeature importance plot saved to 'feature_importance.png'")

# ============================================================================
# STEP 8: SAVE MODEL
# ============================================================================

print("\n" + "=" * 70)
print("STEP 8: Saving Model")
print("=" * 70)

model_path = 'f1_position_predictor.cbm'
final_model.save_model(model_path)
print(f"Model saved to '{model_path}'")

# Save feature names for inference pipeline
import json
with open('feature_names.json', 'w') as f:
    json.dump(feature_cols, f)
print("Feature names saved to 'feature_names.json'")

# ============================================================================
# STEP 9: INFERENCE PIPELINE
# ============================================================================

print("\n" + "=" * 70)
print("STEP 9: Sample Inference Pipeline")
print("=" * 70)

def predict_race_finishing_order(new_data_df, model_path='f1_position_predictor.cbm'):
    """
    Inference pipeline for predicting race finishing order from pre-qualifying features.
    
    Parameters:
    -----------
    new_data_df : pd.DataFrame
        DataFrame with pre-qualifying features (no target column needed)
    model_path : str
        Path to saved CatBoost model
    
    Returns:
    --------
    results_df : pd.DataFrame
        DataFrame with probability distributions and predicted finishing order
    """
    from catboost import CatBoostClassifier
    import json
    
    # Load trained model
    model = CatBoostClassifier()
    model.load_model(model_path)
    
    # Load feature names
    with open('feature_names.json', 'r') as f:
        feature_cols = json.load(f)
    
    # Extract features
    X_new = new_data_df[feature_cols]
    
    # Predict probabilities
    proba = model.predict_proba(X_new)
    
    # Create results DataFrame
    results = pd.DataFrame(
        proba,
        columns=[f'P{i+1}_prob' for i in range(20)]
    )
    
    # Add metadata if available
    if 'driver_id' in new_data_df.columns:
        results['driver_id'] = new_data_df['driver_id'].values
    if 'race_id' in new_data_df.columns:
        results['race_id'] = new_data_df['race_id'].values
    
    # Calculate expected finishing position
    results['expected_position'] = np.sum(proba * np.arange(1, 21), axis=1)
    
    # Most likely position
    results['predicted_position'] = np.argmax(proba, axis=1) + 1
    
    # Sort by expected position to get predicted race order
    results = results.sort_values('expected_position').reset_index(drop=True)
    results['predicted_rank'] = range(1, len(results) + 1)
    
    return results

# Demonstrate inference on a sample race from validation set
print("\nDemonstrating inference on sample race...")
sample_race_id = val_df['race_id'].iloc[0]
sample_race_df = df[df['race_id'] == sample_race_id].copy()

# Run inference
inference_results = predict_race_finishing_order(
    sample_race_df, 
    model_path=model_path
)

print(f"\nPredicted finishing order for race {sample_race_id}:")
print(inference_results[['predicted_rank', 'driver_id', 'expected_position', 
                         'predicted_position']].head(10).to_string(index=False))

# If actual positions available, compare
if 'position' in sample_race_df.columns:
    actual_order = sample_race_df.sort_values('position')[['driver_id', 'position']]
    print(f"\nActual finishing order:")
    print(actual_order.head(10).to_string(index=False))

# ============================================================================
# STEP 10: SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("PIPELINE SUMMARY")
print("=" * 70)

print(f"""
✓ Data split with GroupShuffleSplit (race-level)
✓ Trained CatBoost multiclass classifier
✓ Generated full probability distributions (20 positions)
✓ Evaluated with classification metrics:
  - Accuracy@1: {acc1:.4f}
  - Top-3 Accuracy: {top3_acc:.4f}
  - Log Loss: {logloss:.4f}
✓ Evaluated with ranking metrics:
  - NDCG@20: {ndcg:.4f}
  - MRR: {mrr:.4f}
  - Kendall's Tau: {kendall_tau:.4f}
✓ Saved probability predictions to CSV
✓ Generated feature importance plot
✓ Saved model for inference
✓ Created inference pipeline

Files created:
- predicted_probabilities.csv (probability distributions)
- f1_position_predictor.cbm (trained model)
- feature_names.json (feature list)
- feature_importance.png (visualization)

To use the model for new predictions:
>>> new_predictions = predict_race_finishing_order(your_new_data_df)
""")