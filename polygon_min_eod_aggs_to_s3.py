import os
import dotenv


from datafin import S3Client
from datafin.utils import (
    string_formating,
    format_date
)

dotenv.load_dotenv()

def polygon_min_eod_aggs_to_s3(date_time_object):


    #######################################################
    # Credentials
    #######################################################


    s3_bucket = os.getenv('PERSONAL_S3_BUCKET_NAME')
    aws_access_key =  os.getenv('PERSONAL_AWS_ACCESS_KEY_ID')
    aws_secret_key =  os.getenv('PERSONAL_AWS_SECRET_ACCESS_KEY_ID')

    polygon_access_key =  os.getenv('POLYGON_AWS_ACCESS_KEY_ID')
    polygon_access_key_secret_key =  os.getenv('POLYGON_AWS_SECRET_ACCESS_KEY_ID')


    #######################################################
    # Clients
    #######################################################


    my_s3 = S3Client(
        bucket_name=s3_bucket,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name='us-east-1'
    )

    polygon_s3 = S3Client(
        aws_access_key_id=polygon_access_key,
        aws_secret_access_key=polygon_access_key_secret_key,
        is_polygon=True
    )


    date_uf = date_time_object
    date_f = format_date(date_uf)


    date_year = string_formating(date_uf.year)
    date_month = string_formating(date_uf.month)
    date_day = string_formating(date_uf.day)

    

    polygon_minute_aggs_path = f'us_stocks_sip/minute_aggs_v1/{date_year}/{date_month}'
    polygon_file_name = f'{date_year}-{date_month}-{date_day}'


    polygon_s3_df = polygon_s3.get_csv_compressed(
        path=polygon_minute_aggs_path,
        file_name=polygon_file_name
    )


    my_s3_raw_path = f'dev/polygon/equities/min-eod-aggs/raw/year={date_year}/month={date_month}'
    my_s3_file_name = f'raw-{date_year}-{date_month}-{date_day}'


    my_s3.post_parquet(
        data=polygon_s3_df,
        path=my_s3_raw_path,
        file_name=my_s3_file_name
    )