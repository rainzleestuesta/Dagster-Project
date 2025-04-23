from dagster import ScheduleDefinition
from jobs import stats_job

daily_csv_schedule = ScheduleDefinition(
  job=stats_job,
  cron_schedule="0 9 * * *",
  name="daily_csv_schedule"
)