from DataFin import FMPClient, AWSClient

import os
import dotenv
import time
import time
import pandas as pd
from sqlalchemy import create_engine, text

dotenv.load_dotenv()

fmp_api_key = os.getenv('FMP_API_KEY')


s3_bucket = os.getenv('S3_BUCKET_NAME')
aws_access_key =  os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key =  os.getenv('AWS_SECRET_ACCESS_KEY')

rds_db_username = 'admin'
rds_db_connection_name = os.getenv('RDS_DB_CONNECTION_NAME')
rds_db_password = os.getenv('RDS_DB_PASSWORD')

fmp = FMPClient(fmp_api_key)

rds_engine = create_engine(f"mysql+mysqlconnector://{rds_db_username}:{rds_db_password}@{rds_db_connection_name}/test_db")
conn = rds_engine.connect()

aws = AWSClient(
    aws_access_key,
    aws_secret_key
)

s3 = aws.s3_client(
    'us-east-1'
)

combo_symbols = aws.get_json_from_s3(
    s3,
    s3_bucket,
    'ref-data',
    'combo-snp-nas'
)['data']

def micro_transform(raw_data):
    df = pd.DataFrame(raw_data)
    df['unique_id'] = df['symbol'] + '_' + df['date']
    df['premarket_change'] = df['open'] - df['close'].shift(-1)
    df['premarket_change_percent'] = ((df['open'] - df['close'].shift(-1)) / df['close'].shift(-1)) * 100
    return df 

def write_to_rds(df, table):
    df.to_sql(
        table,
        con=rds_engine,
        if_exists='append', 
        index=False
    )


for symbol in combo_symbols:

    data = aws.get_json_from_s3(
        s3,
        s3_bucket,
        'test-combo-eod-test',
        symbol
    )
    start = time.time()
    df = micro_transform(data)
    write_to_rds(df, 'eod-combo-test-1')
    end = time.time()
    total_time = end - start
    print(f'{symbol:<10} done, took {total_time:>6.2f} seconds')