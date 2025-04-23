from dagster import define_asset_job, AssetSelection

stats_job = define_asset_job(
    name="stats_pipeline",
    selection=AssetSelection.all(),
)
