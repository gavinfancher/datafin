import boto3
from botocore.config import Config
import os
import dotenv
import pandas as pd
import io
from pyarrow import parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta
import concurrent.futures
from functools import lru_cache
import time
from tqdm import tqdm

# Load environment variables once
dotenv.load_dotenv()

# Get environment variables
fmp_api_key = os.getenv('FMP_API_KEY')
s3_bucket = os.getenv('PERSONAL_S3_BUCKET_NAME')
aws_access_key = os.getenv('PERSONAL_AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('PERSONAL_AWS_SECRET_ACCESS_KEY_ID')
polygon_aws_access_key = os.getenv('POLYGON_AWS_ACCESS_KEY_ID')
polygon_aws_secret_key = os.getenv('POLYGON_AWS_SECRET_ACCESS_KEY_ID')

# Initialize FMP client
# fmp = FMPClient(fmp_api_key)

# Create optimized S3 client with connection pooling and retries
@lru_cache(maxsize=2)  # Cache the S3 clients
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

def process_chunk(chunk):
    """Process a chunk of data with timestamp conversion"""
    chunk['date_time'] = pd.to_datetime(chunk['window_start'], unit='ns').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.tz_localize(None)
    return chunk

def write_parquet_to_s3(df, bucket, key, compression='snappy', row_group_size=100000):
    """Write dataframe to S3 as parquet with optimized settings"""
    # Optimize schema for better performance
    schema = pa.Schema.from_pandas(df)
    table = pa.Table.from_pandas(df, schema=schema)
    
    buffer = io.BytesIO()
    pq.write_table(
        table, 
        buffer, 
        compression=compression,
        row_group_size=row_group_size,
        use_dictionary=True,  # Enable dictionary encoding for better compression
        write_statistics=True  # Enable statistics for better query performance
    )
    buffer.seek(0)
    
    personal_s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue()
    )
    print(f"File uploaded to s3://{bucket}/{key}")

try:
    start_time = time.time()
    
    # Download and read the CSV file in chunks
    obj = polygon_s3.get_object(Bucket=bucket_name, Key=object_key)
    chunksize = 100000  # Adjust based on available memory
    
    # Process chunks in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for chunk in pd.read_csv(io.BytesIO(obj['Body'].read()), 
                                compression='gzip', 
                                chunksize=chunksize):
            futures.append(executor.submit(process_chunk, chunk))
        
        # Collect results with progress bar
        processed_chunks = []
        for future in tqdm(concurrent.futures.as_completed(futures), 
                          total=len(futures),
                          desc="Processing chunks"):
            processed_chunks.append(future.result())
    
    # Combine processed chunks
    df = pd.concat(processed_chunks, ignore_index=True)
    
    # Write to S3 with optimized settings
    write_parquet_to_s3(df, s3_bucket, f'dev/parquet-test/{year}-{month}-{yesterday_day}.parquet')
    
    end_time = time.time()
    print(f"Total processing time: {end_time - start_time:.2f} seconds")
    
except Exception as e:
    print(f"Error processing data: {e}")
    raise