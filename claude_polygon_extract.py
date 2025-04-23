import boto3
from botocore.config import Config
import os
import dotenv
import json
import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import concurrent.futures

# Load environment variables once
dotenv.load_dotenv()

# Enhanced S3Client class with connection pooling and better error handling
class S3Client:
    def __init__(self, aws_account_access_key, aws_secret_account_access_key, region, bucket_name):
        self.bucket_name = bucket_name
        # Create a reusable session with connection pooling
        self.session = boto3.Session(
            aws_access_key_id=aws_account_access_key,
            aws_secret_access_key=aws_secret_account_access_key,
            region_name=region
        )
        
        # Create a reusable client with optimized config
        self.client = self.session.client(
            's3',
            config=Config(
                retries={'max_attempts': 10, 'mode': 'adaptive'},
                max_pool_connections=25  # Increase connection pool
            )
        )
    
    def get_object(self, key):
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            return response
        except Exception as e:
            print(f"Error getting object {key}: {e}")
            raise
    
    def put_object(self, key, body):
        try:
            return self.client.put_object(Bucket=self.bucket_name, Key=key, Body=body)
        except Exception as e:
            print(f"Error putting object {key}: {e}")
            raise

# Optimized functions for parquet operations
def write_parquet_to_s3(df, bucket, key, s3_client, compression='snappy', row_group_size=100000):
    """Write dataframe to S3 as parquet with optimized settings"""
    # Convert to PyArrow table with optimized schema
    table = pa.Table.from_pandas(df)
    
    # Use a BytesIO buffer for memory efficiency
    buffer = io.BytesIO()
    
    # Write with optimized settings
    pq.write_table(
        table, 
        buffer, 
        compression=compression,  # snappy offers good balance of speed/compression
        row_group_size=row_group_size  # Optimize row groups for performance
    )
    buffer.seek(0)
    
    # Upload to S3
    s3_client.put_object(key, buffer.getvalue())
    print(f"File uploaded to s3://{bucket}/{key}")

def read_parquet_from_s3(bucket, key, s3_client, columns=None, filters=None):
    """Read parquet from S3 with column projection and filtering"""
    response = s3_client.get_object(key)
    buffer = io.BytesIO(response['Body'].read())
    
    # Use column projection and filters if provided
    table = pq.read_table(buffer, columns=columns, filters=filters)
    return table.to_pandas()

def process_polygon_data():
    # Get environment variables
    s3_bucket = os.getenv('PERSONAL_S3_BUCKET_NAME')
    aws_access_key = os.getenv('PERSONAL_AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('PERSONAL_AWS_SECRET_ACCESS_KEY_ID')
    polygon_aws_access_key = os.getenv('POLYGON_AWS_ACCESS_KEY_ID')
    polygon_aws_secret_key = os.getenv('POLYGON_AWS_SECRET_ACCESS_KEY_ID')
    
    # Set up date calculations
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    year = now.year
    month = f'{now.month:02d}'
    yesterday_day = f'{yesterday.day:02d}'
    
    # Initialize S3 clients - reuse them
    personal_s3 = S3Client(
        aws_account_access_key=aws_access_key,
        aws_secret_account_access_key=aws_secret_key,
        region='us-east-1',
        bucket_name=s3_bucket
    )
    
    # Set up polygon session
    polygon_session = boto3.Session(
        aws_access_key_id=polygon_aws_access_key,
        aws_secret_access_key=polygon_aws_secret_key
    )
    
    polygon_s3 = polygon_session.client(
        's3',
        endpoint_url='https://files.polygon.io',
        config=Config(
            signature_version='s3v4',
            retries={'max_attempts': 10, 'mode': 'adaptive'},
            max_pool_connections=25
        )
    )
    
    # Define file paths
    bucket_name = 'flatfiles'
    object_key = f'us_stocks_sip/minute_aggs_v1/{year}/{month}/{year}-{month}-{yesterday_day}.csv.gz'
    
    try:
        # Get and process data in chunks
        obj = polygon_s3.get_object(Bucket=bucket_name, Key=object_key)
        
        # Process data in chunks to reduce memory usage
        chunks = pd.read_csv(
            io.BytesIO(obj['Body'].read()),
            compression='gzip',
            chunksize=500000  # Process 500k rows at a time
        )
        
        # Pre-allocate a list to store processed chunks
        processed_chunks = []
        
        for chunk in chunks:
            # Optimize datetime conversion (more efficient than chained operations)
            chunk['date_time'] = pd.to_datetime(chunk['window_start'], unit='ns')
            # Use vectorized string operations instead of chained dt accessors
            processed_chunks.append(chunk)
        
        # Concatenate once at the end (more efficient)
        df = pd.concat(processed_chunks, ignore_index=True)
        
        # Convert timezone in a single operation
        df['date_time'] = df['date_time'].dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.tz_localize(None)
        
        # Write to S3 with optimized settings
        output_key = f'dev/parquet-test/{year}-{month}-{yesterday_day}.parquet'
        write_parquet_to_s3(df, s3_bucket, output_key, personal_s3, compression='snappy')
        
        # Read back only if needed
        return output_key
        
    except Exception as e:
        print(f"Error processing data: {e}")
        raise

# Parallel processing example (if needed)
def process_multiple_days(days_back=7):
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        now = datetime.now()
        
        for i in range(1, days_back + 1):
            date = now - timedelta(days=i)
            # Set up function to process each day
            futures.append(executor.submit(process_day, date))
            
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                print(f"Processed: {result}")
            except Exception as e:
                print(f"Error in processing: {e}")

def process_day(date):
    # Implementation to process a specific day
    # Similar to process_polygon_data but for a specific date
    pass

if __name__ == "__main__":
    process_polygon_data()