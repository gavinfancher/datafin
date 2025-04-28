import os
import dotenv


from datafin import S3Client                     # type: ignore
from datafin import FMPClient                    # type: ignore
from datafin import PolygonClient                # type: ignore
from datafin.utils import (                      # type: ignore
    get_trading_days_ytd,
    get_trading_days_range,
    format_date,
    today,
    get_5year_ago_date
)

dotenv.load_dotenv()

#######################################################
# Credentials
#######################################################

fmp_api_key = os.getenv('FMP_API_KEY')

s3_bucket = os.getenv('PERSONAL_S3_BUCKET_NAME')
aws_access_key =  os.getenv('PERSONAL_AWS_ACCESS_KEY_ID')
aws_secret_key =  os.getenv('PERSONAL_AWS_SECRET_ACCESS_KEY_ID')

polygon_api_key = os.getenv('POLYGON_API_KEY')
polygon_access_key =  os.getenv('POLYGON_AWS_ACCESS_KEY_ID')
polygon_access_key_secret_key =  os.getenv('POLYGON_AWS_SECRET_ACCESS_KEY_ID')


#######################################################
# S3 Client Test
#######################################################


my_s3 = S3Client(
    bucket_name=s3_bucket,
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name='us-east-1'
)

s3_test = my_s3.get_json(
    'test',
    'test'
)
print('\n' * 5)
print(s3_test)


#######################################################
# FMP Client Test
#######################################################


fmp_client = FMPClient(
    api_key=fmp_api_key
)

fmp_test = fmp_client.get_quote(
    'AAPL'
)
print('\n' * 5)
print(fmp_test)


#######################################################
# Polygon Client Test
#######################################################


polygon_client = PolygonClient(
    api_key=polygon_api_key
)

polygon_test = polygon_client.get_eod_aggs(
    symbol='AAPL',
    date='2025-01-10'
)
print('\n' * 5)
print(polygon_test)


#######################################################
# Datetime Utils Test
#######################################################


ytd_trading_days = get_trading_days_ytd()
print('\n' * 5)
print(ytd_trading_days[:3])

trading_day_range = get_trading_days_range('2000-01-01', '2020-01-01')
print('\n' * 5)
print(trading_day_range[:3])

today_date = today()
formatted_today_date = format_date(today_date)
print('\n' * 5)
print(formatted_today_date)

five_year_ago_date = get_5year_ago_date()
print('\n' * 5)
print(five_year_ago_date)