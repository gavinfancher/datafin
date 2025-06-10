from dagster import Definitions

#jobs
from .jobs.full_market_aggs import full_market_minute_aggs_job
from .jobs.spy_second_aggs_job import spy_second_agg_job


#schdules
from .schedules.full_market_scedules import get_polygon_whole_market_minute_aggs_schedule
from .schedules.spy_schedules import spy_every_second

defs = Definitions(
    jobs=[
        full_market_minute_aggs_job,
        spy_second_agg_job
    ],
    schedules=[
        get_polygon_whole_market_minute_aggs_schedule,
        spy_every_second
    ]
)