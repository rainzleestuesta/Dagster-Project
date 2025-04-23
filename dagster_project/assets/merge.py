from dagster import asset
import pandas as pd

@asset
def merged_stats() -> pd.DataFrame:
    file_path = "preprocess/merged_players_team_stats.csv"
    df = pd.read_csv(file_path)
    return df
