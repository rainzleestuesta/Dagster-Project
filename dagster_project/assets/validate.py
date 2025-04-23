from dagster import asset
import pandas as pd

@asset
def validated_stats(merged_stats: pd.DataFrame) -> pd.DataFrame:
    """
    Validate merged player-team stats:
    - No null values in key columns.
    - win_percentage must be between 0.0 and 1.0.
    - pts_per_game must be within expected range (0–40).
    """

    required_columns = ["player", "team_code", "pts_per_game", "win_percentage"]

    # Check for missing values
    missing = merged_stats[required_columns].isnull().sum()
    if missing.any():
        raise ValueError(f"Missing values in required columns:\n{missing}")

    # Validate win_percentage is between 0.0 and 1.0
    if not merged_stats["win_percentage"].between(0, 1).all():
        bad_rows = merged_stats[~merged_stats["win_percentage"].between(0, 1)]
        raise ValueError(f"win_percentage out of bounds:\n{bad_rows[['player', 'win_percentage']]}")

    # Validate pts_per_game is reasonable (e.g. 0 to 40)
    if not merged_stats["pts_per_game"].between(0, 40).all():
        bad_pts = merged_stats[~merged_stats["pts_per_game"].between(0, 40)]
        raise ValueError(f"Unexpected pts_per_game values:\n{bad_pts[['player', 'pts_per_game']]}")

    return merged_stats
