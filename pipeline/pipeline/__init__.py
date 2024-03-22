from dagster import Definitions

from .assets import load_items, scheduled_job

defs = Definitions(
    jobs=[load_items],
    schedules=[scheduled_job]
)
