from dagster import asset, job, op, materialize, schedule, Definitions, RunRequest

import pandas as pd
from datetime import datetime


from datafin.aws import S3Client                           #type: ignore
from datafin.apis import PolygonClient                     #type: ignore
from datafin.utils import (                                #type: ignore
    now,
    to_ny_time,
    string_formating,
    get_ny_timestamp_for_today_time_range
)                                

from datafin_dagster.resources.credentials import SecretsResource

@op
def polygon_spy_close_minute_raw_df(
        secrets: SecretsResource
):

    pg = PolygonClient(
        secrets.client.get_polygon_api_key()
    )

    timestamps = get_ny_timestamp_for_today_time_range(
        _from = (15, 59, 0),
        _to = (15, 59, 59)
    )

    spy_closing_min = pg.get_aggs(
        symbol='spy',
        multiplier=1,
        unit='second',
        _from = timestamps[0],
        _to = timestamps[1]
    )
    df = pd.DataFrame(spy_closing_min)
    return df

@op
def polygon_spy_close_minute_clean_df(
        polygon_spy_close_minute_raw_df: pd.DataFrame
):
    df = polygon_spy_close_minute_raw_df.drop(columns=['otc'])
    df['datetime_ny'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True).dt.tz_convert('America/New_York')
    return df

@op
def polygon_spy_close_clean_df_post_to_s3(
        polygon_spy_close_minute_clean_df: pd.DataFrame,
        secrets: SecretsResource
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

    ny_dt = to_ny_time(now())
    date_year = string_formating(ny_dt.year)
    date_month = string_formating(ny_dt.month)
    date_day = string_formating(ny_dt.day)

    my_s3_raw_path = f'dev/polygon/equities/spy/store/market-close/year={date_year}'
    my_s3_file_name = f'spy-close-{date_year}-{date_month}-{date_day}'

    my_s3.post_parquet(
        data=polygon_spy_close_minute_clean_df,
        path=my_s3_raw_path,
        file_name=my_s3_file_name
    )

    return None

@job
def polygon_spy_close_minute_job():
    polygon_spy_close_clean_df_post_to_s3(
        polygon_spy_close_minute_clean_df(
            polygon_spy_close_minute_raw_df()
        )
    )

@schedule(
    job = polygon_spy_close_minute_job,
    cron_schedule = '20 16 * * *',
    execution_timezone ='America/New_York'
)
def polygon_spy_close_minute_schedule():
    """
    comment here
    """
    return [RunRequest(run_key=None)]


polygon_spy_close_minute_definition = Definitions(
    jobs=[polygon_spy_close_minute_job],
    schedules=[polygon_spy_close_minute_schedule]
)