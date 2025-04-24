import os
import dotenv

from datafin import S3Client

dotenv.load_dotenv()

fmp_api_key = os.getenv('FMP_API_KEY')

s3_bucket = os.getenv('PERSONAL_S3_BUCKET_NAME')
aws_access_key =  os.getenv('PERSONAL_AWS_ACCESS_KEY_ID')
aws_secret_key =  os.getenv('PERSONAL_AWS_SECRET_ACCESS_KEY_ID')

my_s3 = S3Client(
    bucket_name=s3_bucket,
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name='us-east-1',
)

forex = my_s3.get_json(
    'ref-data',
    'forex-pairs'
)

print(forex)