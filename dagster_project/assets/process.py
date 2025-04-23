# assets/processing.py

from dagster import asset
import pandas as pd

@asset
def processed_stats(validated_stats: pd.DataFrame) -> pd.DataFrame:
    # Compute per-player z-scores for PTS, AST, TRB and combine into an all-around score.

    df = validated_stats.copy()

    # 1) compute league mean & std
    stats = ["pts_per_game", "ast_per_game", "trb_per_game"]
    means = df[stats].mean()
    stds  = df[stats].std()

    # 2) z-score each stat
    for stat in stats:
        df[f"{stat}_z"] = (df[stat] - means[stat]) / stds[stat]

    # 3) composite all-around score
    df["all_around_score"] = df[[f"{s}_z" for s in stats]].sum(axis=1)

    # 4) rank within league
    df = df.sort_values("all_around_score", ascending=False)
    return df
