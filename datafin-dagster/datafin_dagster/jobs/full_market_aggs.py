from dagster import asset, job


from datafin.aws import S3Client                     #type: ignore
from datafin.utils import string_formating       #type: ignore

# from datafin_dagster.resources.constants import HI
import pandas as pd
from datetime import datetime
import os


## want to change how i handle secrets and constants for whole project not just in this dagster file


# personal credentials
aws_access_key =  os.getenv('PERSONAL_AWS_ACCESS_KEY_ID')
aws_secret_key =  os.getenv('PERSONAL_AWS_SECRET_ACCESS_KEY_ID')
s3_bucket_name = os.getenv('PERSONAL_S3_BUCKET_NAME')


# polygon related credentials
polygon_access_key =  os.getenv('POLYGON_AWS_ACCESS_KEY_ID')
polygon_access_key_secret_key =  os.getenv('POLYGON_AWS_SECRET_ACCESS_KEY_ID')


@asset()
def polygon_full_market_minute_aggs() -> pd.DataFrame:
    """
    commment about function
    """

    polygon_s3 = S3Client(
        aws_access_key_id=polygon_access_key,
        aws_secret_access_key=polygon_access_key_secret_key,
        is_polygon=True
    )


    date_uf = datetime.now()
    date_year = string_formating(date_uf.year)
    date_month = string_formating(date_uf.month)
    date_day = string_formating(date_uf.day)

    

    polygon_minute_aggs_path = f'us_stocks_sip/minute_aggs_v1/{date_year}/{date_month}'
    polygon_file_name = f'{date_year}-{date_month}-{date_day}'


    return polygon_s3.get_csv_compressed(
        path=polygon_minute_aggs_path,
        file_name=polygon_file_name
    )


@asset()
def stored_polygon_full_market_minute_aggs(polygon_full_market_minute_aggs: pd.DataFrame):
    """
    comment on function
    """

    my_s3 = S3Client(
        bucket_name=s3_bucket_name,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name='us-east-1'
    )

    date_uf = datetime.now()
    date_year = string_formating(date_uf.year)
    date_month = string_formating(date_uf.month)
    date_day = string_formating(date_uf.day)

    # Define S3 path and filename
    my_s3_raw_path = f'dev/polygon/equities/full-market-aggs/raw/year={date_year}/month={date_month}'
    my_s3_file_name = f'raw-{date_year}-{date_month}-{date_day}'

    start_time =datetime.now()


    my_s3.post_parquet(
        data=polygon_full_market_minute_aggs,
        path=my_s3_raw_path,
        file_name=my_s3_file_name
    )

    end_time = datetime.now()

    return []


@job
def full_market_minute_aggs_job():
    stored_polygon_full_market_minute_aggs(polygon_full_market_minute_aggs())


