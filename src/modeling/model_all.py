# Jack Wilson
# 2/11/2026
# This file runs all modeling functions

# --------------------------------------------------------------------------------
# Import modules

import os, sys
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(current_dir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.modeling.model_pre_qual import model_pre_qual
from src.modeling.model_pre_race import model_pre_race
from src.modeling.model_utils import driver_percentages_dataframe
from src.utils.utils import read_run_stage, update_run_stage


# --------------------------------------------------------------------------------

def model_all(from_file=True, grand_prix=None, train_new_model=True,
              save_results=True, verbose=False, timestamp=False):
    """
    Run both pre-qualifying and pre-race prediction models.

    Parameters:
    -----------
    from_file : bool
        If True, read drivers/teams/grand_prix from driver_pred_list.xlsx;
        driver count = number of rows with a driver name. If False, use
        drivers/teams from the most recent race (same count as that race).
    grand_prix : str, optional
        Required if from_file=False. Grand prix name to predict.
    train_new_model : bool
        If True, train new models. If False, load existing models.
    save_results : bool
        If True, save prediction results to CSV files.
    verbose : bool
        Whether to print training progress.
    timestamp : bool
        If True, check .last_run.json and skip if model stage was run recently;
        after running, update the model stage timestamp.

    Returns:
    --------
    results : dict
        Dictionary with 'pre_qual' and 'pre_race' keys, each containing
        (driver_tables, race_outcome) tuples.
    """
    if timestamp is True:
        if read_run_stage("model") is False:
            print("\nNo modeling updates needed")
            return {}

    results = {}

    # Pre-qualifying predictions (practice only)
    print("Making pre-qualifying predictions (practice data only)...")
    driver_tables_pre_qual, race_outcome_pre_qual = model_pre_qual(
        from_file=from_file,
        grand_prix=grand_prix,
        train_new_model=train_new_model,
        verbose=verbose
    )
    results["pre_qual"] = (driver_tables_pre_qual, race_outcome_pre_qual)

    # Pre-race predictions (practice + qualifying)
    print("\n" + "="*70)
    print("Making pre-race predictions (practice + qualifying)...")
    print("="*70)
    driver_tables_pre_race, race_outcome_pre_race = model_pre_race(
        from_file=from_file,
        grand_prix=grand_prix,
        train_new_model=train_new_model,
        verbose=verbose
    )
    results["pre_race"] = (driver_tables_pre_race, race_outcome_pre_race)

    # Save results to CSV
    if save_results:
        output_dir = os.path.join(PROJECT_ROOT, "src", "pipeline")
        os.makedirs(output_dir, exist_ok=True)

        # Race outcomes
        race_outcome_pre_qual.to_csv(
            os.path.join(output_dir, "pre_qual_race_outcome.csv"), index=False
        )
        race_outcome_pre_race.to_csv(
            os.path.join(output_dir, "pre_race_race_outcome.csv"), index=False
        )

        # Single file per model with all driver position percentages (Driver, Team, P1, P2, ...)
        drivers_ordered = list(driver_tables_pre_qual.keys())
        team_map = dict(zip(race_outcome_pre_qual["Driver"], race_outcome_pre_qual["Team"]))
        teams_ordered = [team_map.get(d, "") for d in drivers_ordered]
        pre_qual_pct = driver_percentages_dataframe(
            driver_tables_pre_qual, drivers_ordered, teams_ordered
        )
        pre_race_pct = driver_percentages_dataframe(
            driver_tables_pre_race, drivers_ordered, teams_ordered
        )
        pre_qual_pct.to_csv(
            os.path.join(output_dir, "pre_qual_driver_percentages.csv"), index=False
        )
        pre_race_pct.to_csv(
            os.path.join(output_dir, "pre_race_driver_percentages.csv"), index=False
        )

        print(f"\nPrediction results saved")

    if timestamp is True:
        update_run_stage("model")

    return results


if __name__ == "__main__":
    model_all(
        from_file=True,
        train_new_model=True,
        save_results=True,
        verbose=True,
        timestamp=False,
    )
