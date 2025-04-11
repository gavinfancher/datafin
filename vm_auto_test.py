from DataFin import FMPClient, AWSClient

import os
import dotenv

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
)

nasdaq