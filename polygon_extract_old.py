import boto3
from botocore.config import Config

from DataFin import FMPClient, S3Client

import os
import dotenv
import boto3
import json
import pandas as pd
import io
from pyarrow import parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta

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


personal_s3 = S3Client(
    aws_account_access_key = aws_access_key,
    aws_secret_account_access_key = aws_secret_key,
    region = 'us-east-1',
    bucket_name = s3_bucket
)


df['date_time'] = pd.to_datetime(df['window_start'], unit='ns').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.tz_localize(None)



import pandas as pd
import boto3
import io
from pyarrow import parquet as pq
import pyarrow as pa



def write_parquet_to_s3(df, bucket, key):
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    
    pq.write_table(table, buffer)
    buffer.seek(0)
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id= aws_access_key,
        aws_secret_access_key= aws_secret_key,
        region_name='us-east-1'
    )
    
    s3_client.upload_fileobj(buffer, bucket, key)
    print(f"File uploaded to s3://{bucket}/{key}")



def read_parquet_with_boto3(bucket, key):

    s3_client = boto3.client(
        's3',
        aws_access_key_id= aws_access_key,
        aws_secret_access_key= aws_secret_key,
        region_name='us-east-1'
    )
    
    response = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )
    
    buffer = io.BytesIO(response['Body'].read())
    
    table = pq.read_table(buffer)
    
    df = table.to_pandas()
    
    return df


write_parquet_to_s3(df, s3_bucket, 'dev/parquet-test/2025-04-21.parquet')


df_p = read_parquet_with_boto3(s3_bucket, 'dev/parquet-test/2025-04-21.parquet')

df_p

# Create optimized S3 client with connection pooling and retries
def create_s3_client(access_key, secret_key, endpoint_url=None):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    return session.client(
        's3',
        endpoint_url=endpoint_url,
        config=Config(
            signature_version='s3v4',
            retries={'max_attempts': 10, 'mode': 'adaptive'},
            max_pool_connections=25  # Increase connection pool
        )
    )

# Create S3 clients
personal_s3 = create_s3_client(aws_access_key, aws_secret_key)
polygon_s3 = create_s3_client(polygon_aws_access_key, polygon_aws_secret_key, 'https://files.polygon.io')

# Get current date information
now = datetime.now()
yesterday = now - timedelta(days=1)
year = now.year
month = f'{now.month:02d}'
yesterday_day = f'{yesterday.day:02d}'

# Define file paths
bucket_name = 'flatfiles'
object_key = f'us_stocks_sip/minute_aggs_v1/{year}/{month}/{year}-{month}-{yesterday_day}.csv.gz'

try:
    # Download and read the CSV file
    obj = polygon_s3.get_object(Bucket=bucket_name, Key=object_key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), compression='gzip')
    
    # Convert timestamp with optimized method
    df['date_time'] = pd.to_datetime(df['window_start'], unit='ns').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.tz_localize(None)
    
    # Optimized parquet writing function
    def write_parquet_to_s3(df, bucket, key, compression='snappy', row_group_size=100000):
        """Write dataframe to S3 as parquet with optimized settings"""
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        
        pq.write_table(
            table, 
            buffer, 
            compression=compression,
            row_group_size=row_group_size
        )
        buffer.seek(0)
        
        personal_s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue()
        )
        print(f"File uploaded to s3://{bucket}/{key}")
    
    # Write to S3
    write_parquet_to_s3(df, s3_bucket, f'dev/parquet-test/{year}-{month}-{yesterday_day}.parquet')
    
except Exception as e:
    print(f"Error processing data: {e}")
    raise