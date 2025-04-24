from DataFin_old import FMPClient, S3Client

import os
import dotenv
import time

global_start = time.time()
dotenv.load_dotenv()

fmp_api_key = os.getenv('FMP_API_KEY')

s3_bucket = os.getenv('S3_BUCKET_NAME')
aws_access_key =  os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key =  os.getenv('AWS_SECRET_ACCESS_KEY')


fmp = FMPClient(fmp_api_key)

s3 = S3Client(
    aws_account_access_key = aws_access_key,
    aws_secret_account_access_key = aws_secret_key,
    region = 'us-east-1',
    bucket_name = s3_bucket
)


pairs = s3.json_get(
    'ref-data',
    'forex-pairs'
)['data']

for p in pairs:
    start = time.time()
    data = fmp.get_eod_forex(p)
    
    s3.json_post(
        data,
        'dev/forex/eod/day0',
        f'{p}'
    )
    total_time = time.time() - start
    print(f'{p:<10} done | took {total_time:>6.2f} seconds')

global_total_time = time.time() - global_start
print(f'job finished | took  {global_total_time:>6.2f} seconds')