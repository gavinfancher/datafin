from DataFin import FMPClient, AWSClient

import os
import dotenv
import time

dotenv.load_dotenv()

## load environment variables from .env
fmp_api_key = os.getenv('FMP_API_KEY')


s3_bucket = os.getenv('S3_BUCKET_NAME')
aws_access_key =  os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key =  os.getenv('AWS_SECRET_ACCESS_KEY')


fmp = FMPClient(fmp_api_key)

aws = AWSClient(
    aws_access_key,
    aws_secret_key
)

s3 = aws.s3_client('us-east-1')

nasdaq = aws.get_json_from_s3(
    s3,
    s3_bucket,
    'ref-data',
    'nasdaq'
)['data']

snp = aws.get_json_from_s3(
    s3,
    s3_bucket,
    'ref-data',
    'snp500'
)['data']


# amzn_data = fmp.get_end_of_day_full('amzn')

# aws.post_json_to_s3(
#     s3,
#     amzn_data,
#     s3_bucket,
#     'test-eod-data',
#     'amzn'
# )


for symbol in snp:
    start = time.time()
    data = fmp.get_end_of_day_full(symbol)
    aws.post_json_to_s3(
        s3,
        data,
        s3_bucket,
        'test-eod-data-local',
        f'{symbol}'
    )
    end = time.time()
    elapsed_time = end - start
    print(f'wrote {symbol} to s3 -- took {elapsed_time:.2f}s')