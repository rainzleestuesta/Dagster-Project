from dagster import Definitions, AssetSelection, define_asset_job, with_resources
from assets.merge import merged_stats
from assets.validate import validated_stats
from assets.process import processed_stats
from assets.insights import (
    top_scorers_chart,
    assist_leaders_chart,
    best_team_offense_chart,
    net_rating_vs_win_scatter,
    top_all_around_players_chart,
    team_pace_bar_chart
)

from io_managers import csv_output_io_manager
from schedules import daily_csv_schedule, stats_job

# Only assign the IO manager to the final output asset
validated_stats_wrapped = with_resources(
    [validated_stats],
    {"io_manager": csv_output_io_manager}
)

all_assets = [
    merged_stats,
    *validated_stats_wrapped, 
    processed_stats,
    top_scorers_chart,
    assist_leaders_chart,
    best_team_offense_chart,
    net_rating_vs_win_scatter,
    top_all_around_players_chart,
    team_pace_bar_chart
]

defs = Definitions(
    assets=all_assets,
    jobs=[stats_job],
    schedules=[daily_csv_schedule],
    resources={"io_manager": csv_output_io_manager},  # ✅ make it global
)

