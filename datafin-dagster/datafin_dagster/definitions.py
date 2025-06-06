from dagster import Definitions

from .jobs.spy_daily_job import get_spy_job
# from .schedules.spy_schedules import (
#     morning_spy_schedule,
#     afternoon_spy_schedule,
#     evening_spy_schedule
# )

defs = Definitions(
    jobs=[get_spy_job],
    # schedules=[
    #     morning_spy_schedule,
    #     afternoon_spy_schedule, 
    #     evening_spy_schedule
    # ]
)