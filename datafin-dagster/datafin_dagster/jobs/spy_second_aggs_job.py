from dagster import job, op, asset, In, Out


from datafin import PolygonClient    #type: ignore
from datafin.utils import (      #type: ignore
    format_date
)

from datafin_dagster.resources.constants import (
    POLYGON_API_KEY,
    # FMP_API_KEY
    # MY_S3_BUCKET,
    # PERSONAL_AWS_KEY,
    # PERSONAL_AWS_SECRET_KEY

)
import pandas as pd
from datetime import datetime


@asset
def spy_second_polygon_api_response():


    polygon_api_key = POLYGON_API_KEY

    today = datetime.now()

    formatted_date = format_date(today)

    pg = PolygonClient(
        api_key = polygon_api_key
    )

    data = pg.get_eod_second_aggs(
        'SPY',
        formatted_date
    )
    return data


# @op
# def posted_daily_spy_to_s3(data):


@job
def spy_second_agg_job():
    raw_data = spy_second_polygon_api_response()