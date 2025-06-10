from dagster import Definitions, EnvVar

#jobs
from .jobs.full_market_aggs import full_market_minute_aggs_job


#schdules
from .schedules.full_market_scedules import get_polygon_whole_market_minute_aggs_schedule


#resources
from .resources.credentials import SecretsResource


defs = Definitions(
    jobs=[
        full_market_minute_aggs_job
    ],
    schedules=[
        get_polygon_whole_market_minute_aggs_schedule
    ],
    resources={
        "secrets": SecretsResource(
            aws_access_key=EnvVar("PERSONAL_AWS_ACCESS_KEY"),
            aws_secret_access_key=EnvVar("PERSONAL_AWS_SECRET_ACCESS_KEY")
        )
    }
)
