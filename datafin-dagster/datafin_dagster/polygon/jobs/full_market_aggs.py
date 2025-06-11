from dagster import asset, op, job, materialize, schedule, Definitions, RunRequest, define_asset_job
import pandas as pd
from datetime import datetime


from datafin.aws import S3Client                           #type: ignore
from datafin.utils import (                                #type: ignore
    to_ny_time,
    string_formating,
    yesterday
)                                

from datafin_dagster.resources.credentials import SecretsResource

@op
def polygon_full_market_minute_aggs(secrets: SecretsResource) -> pd.DataFrame:
    """
    commment about function
    """
    polygon_s3 = S3Client(
        aws_access_key_id=secrets.client.get_polygon_aws_key(),
        aws_secret_access_key=secrets.client.get_polygon_api_key(),
        is_polygon=True
    )

    ny_dt = to_ny_time(yesterday())
    date_year = string_formating(ny_dt.year)
    date_month = string_formating(ny_dt.month)
    date_day = string_formating(ny_dt.day)

    polygon_minute_aggs_path = f'us_stocks_sip/minute_aggs_v1/{date_year}/{date_month}'
    polygon_file_name = f'{date_year}-{date_month}-{date_day}'

    return polygon_s3.get_csv_compressed(
        path=polygon_minute_aggs_path,
        file_name=polygon_file_name
    )


@op
def polygon_stored_full_market_minute_aggs(
    secrets: SecretsResource,
    polygon_full_market_minute_aggs: pd.DataFrame
    
) -> None:
    """
    comment on function
    """

    my_s3 = S3Client(
        bucket_name=secrets.client.get_bucket_name(),
        aws_access_key_id=secrets.client.aws_access_key,
        aws_secret_access_key=secrets.client.aws_secret_access_key,
        region_name='us-east-1'
    )

    ny_dt = to_ny_time(yesterday())
    date_year = string_formating(ny_dt.year)
    date_month = string_formating(ny_dt.month)
    date_day = string_formating(ny_dt.day)

    my_s3_raw_path = f'dev/polygon/equities/full-market-aggs/raw/year={date_year}/month={date_month}'
    my_s3_file_name = f'raw-{date_year}-{date_month}-{date_day}'

    my_s3.post_parquet(
        data=polygon_full_market_minute_aggs,
        path=my_s3_raw_path,
        file_name=my_s3_file_name
    )

    return None


@job
def polygon_full_market_minute_aggs_job():
    polygon_stored_full_market_minute_aggs(
        polygon_full_market_minute_aggs()
    )


@schedule(
    job = polygon_full_market_minute_aggs_job,
    cron_schedule = '0 2 * * *',
    execution_timezone ='America/New_York'
)
def get_polygon_whole_market_minute_aggs_schedule():
    """
    comment here
    """
    return [RunRequest(run_key=None)]



polygon_full_market_aggs_definition = Definitions(
    jobs=[polygon_full_market_minute_aggs_job],
    schedules=[get_polygon_whole_market_minute_aggs_schedule]
)