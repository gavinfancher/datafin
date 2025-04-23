import boto3
from botocore.config import Config

from DataFin import FMPClient, S3Client

import os
import dotenv
import boto3
import json
import pandas as pd
import io

dotenv.load_dotenv()

fmp_api_key = os.getenv('FMP_API_KEY')

s3_bucket = os.getenv('PERSONAL_S3_BUCKET_NAME')
aws_access_key =  os.getenv('PERSONAL_AWS_ACCESS_KEY_ID')
aws_secret_key =  os.getenv('PERSONAL_AWS_SECRET_ACCESS_KEY_ID')


fmp = FMPClient(fmp_api_key)

personal_s3 = S3Client(
    aws_account_access_key = aws_access_key,
    aws_secret_account_access_key = aws_secret_key,
    region = 'us-east-1',
    bucket_name = s3_bucket
)

polygon_aws_access_key = os.getenv('POLYGON_AWS_ACCESS_KEY_ID')
polygon_aws_secret_key = os.getenv('POLYGON_AWS_SECRET_ACCESS_KEY_ID')


session = boto3.Session(
   aws_access_key_id = polygon_aws_access_key,
   aws_secret_access_key = polygon_aws_secret_key
)

s3 = session.client(
   's3',
   endpoint_url='https://files.polygon.io',
   config=Config(signature_version='s3v4'),
)



from datetime import datetime, timedelta
now = datetime.now()
yesterday = now - timedelta(days=1)

year = now.year  
month = f'{now.month:02d}'
day = f'{now.day:02d}'
yesterday = f'{yesterday.day:02d}'
print(month, day, yesterday)


bucket_name = 'flatfiles'
object_key = f'us_stocks_sip/minute_aggs_v1/2025/{month}/2025-{month}-{yesterday}.csv.gz'

obj = s3.get_object(Bucket=bucket_name, Key=object_key)


df = pd.read_csv(io.BytesIO(obj['Body'].read()), compression='gzip')

df['date_time'] = pd.to_datetime(df['window_start'], unit='ns').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.tz_localize(None)

df.to_parquet(f'2025-04-{yesterday}-test.parquet')