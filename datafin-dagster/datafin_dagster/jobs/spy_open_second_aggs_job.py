from dagster import job, op, asset


from datafin.apis import PolygonClient                     #type: ignore
from datafin.aws import SecretsClient                      #type: ignore
from datafin.utils import (                                #type: ignore
    format_date
)


import pandas as pd
from datetime import datetime






@asset
def spy_second_polygon_api_response():

    secrets = SecretsClient()
    polygon_api_key = secrets.get_polygon_api_key()

    today = datetime.now()
    formatted_date = format_date(today)

    pg = PolygonClient(
        api_key = polygon_api_key
    )

    data = pg.get_eod_second_aggs(
        'SPY',
        formatted_date
    )
    raw_df = pd.DataFrame(data)


@op
def posted_daily_spy_to_s3(data):
    

# @job
# def spy_second_agg_job():
#     raw_data = spy_second_polygon_api_response()