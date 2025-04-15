from DataFin import FMPClient, AWSClient

import os
import dotenv
import time

dotenv.load_dotenv()


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

combo_symbols = aws.get_json_from_s3(
    s3,
    s3_bucket,
    'ref-data',
    'combo-snp-nas'
)['data']

for symbol in combo_symbols:
    start = time.time()
    data = fmp.get_end_of_day_full(symbol)
    aws.post_json_to_s3(
        s3,
        data,
        s3_bucket,
        'test-combo-eod-test',
        f'{symbol}'
    )
    end = time.time()
    elapsed_time = end - start
    print(f'{symbol:<10} done, took {elapsed_time:>6.2f} seconds')