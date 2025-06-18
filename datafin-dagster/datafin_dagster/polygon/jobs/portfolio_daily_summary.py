from typing import list
import json

from dagster import asset, job, op, materialize, schedule, Definitions, RunRequest

import pandas as pd
from datetime import datetime


from datafin.aws import S3Client                           #type: ignore
from datafin.apis import PolygonClient                     #type: ignore
from datafin.utils import GmailClient                      #type: ignore
from datafin.utils import (                                #type: ignore
    now,
    to_ny_time,
    string_formating,
    get_ny_timestamp_for_today_time_range,
    is_today_a_trading_day
)                                

from datafin_dagster.resources.credentials import SecretsResource

@op
def portfolio_list(
        secrets: SecretsResource
) -> list:
    
    s3 = S3Client(
        bucket_name=secrets.client.get_bucket_name(),
        aws_access_key_id=secrets.client.aws_access_key,
        aws_secret_access_key=secrets.client.aws_secret_access_key,
        region_name='us-east-1'
    )

    raw_json_list = s3.get_json(
        path = 'v1/reference',
        file_name = 'portfolio'
    )

    cleaned_list = list(json.loads(raw_json_list)['data'])
    

@op
def portfolio_list(
        secrets: SecretsResource
):

    pg = PolygonClient(
        secrets.client.get_polygon_api_key()
    )

    timestamps = get_ny_timestamp_for_today_time_range(
        _from = (9, 30, 0),
        _to = (9, 30, 59)
    )

    spy_opening_min = pg.get_aggs(
        symbol='spy',
        multiplier=1,
        unit='second',
        _from = timestamps[0],
        _to = timestamps[1]
    )
    df = pd.DataFrame(spy_opening_min)
    return df

@op
def polygon_spy_open_minute_clean_df(
        polygon_spy_open_minute_raw_df: pd.DataFrame
):
    df = polygon_spy_open_minute_raw_df.drop(columns=['otc'])
    df['datetime_ny'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True).dt.tz_convert('America/New_York')
    return df

@op
def polygon_spy_open_clean_df_post_to_s3(
        polygon_spy_open_minute_clean_df: pd.DataFrame,
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

    my_s3_raw_path = f'dev/polygon/equities/spy/store/market-open/year={date_year}'
    my_s3_file_name = f'spy-open-{date_year}-{date_month}-{date_day}'

    my_s3.post_parquet(
        data=polygon_spy_open_minute_clean_df,
        path=my_s3_raw_path,
        file_name=my_s3_file_name
    )

    return None

@job
def portfolio_daily_summary_job():
    pass

@schedule(
    job = portfolio_daily_summary_job,
    cron_schedule = '20 16 * * *',
    execution_timezone ='America/New_York'
)
def portfolio_daily_summary_schedule():
    """
    comment here
    """
    if is_today_a_trading_day():
        return [RunRequest(run_key=None)]
    else:
        return []


portfolio_daily_summary_definition = Definitions(
    jobs=[portfolio_daily_summary_job],
    schedules=[portfolio_daily_summary_schedule]
)