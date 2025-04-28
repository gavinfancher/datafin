import os
import dotenv


from datafin import S3Client                     # type: ignore
from datafin.utils import (                      # type: ignore
    get_trading_days_ytd,
    get_trading_days_range,
    format_date,
    today,
    get_5year_ago_date
)

dotenv.load_dotenv()


s3_bucket = os.getenv('PERSONAL_S3_BUCKET_NAME')
aws_access_key =  os.getenv('PERSONAL_AWS_ACCESS_KEY_ID')
aws_secret_key =  os.getenv('PERSONAL_AWS_SECRET_ACCESS_KEY_ID')


polygon_access_key =  os.getenv('POLYGON_AWS_ACCESS_KEY_ID')
polygon_access_key_secret_key =  os.getenv('POLYGON_AWS_SECRET_ACCESS_KEY_ID')



my_s3 = S3Client(
    bucket_name=s3_bucket,
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name='us-east-1'
)

test = my_s3.get_json(
    'test',
    'test'
)
print(test)

dotenv.load_dotenv()


s3_bucket = os.getenv('PERSONAL_S3_BUCKET_NAME')
aws_access_key =  os.getenv('PERSONAL_AWS_ACCESS_KEY_ID')
aws_secret_key =  os.getenv('PERSONAL_AWS_SECRET_ACCESS_KEY_ID')


polygon_access_key =  os.getenv('POLYGON_AWS_ACCESS_KEY_ID')
polygon_access_key_secret_key =  os.getenv('POLYGON_AWS_SECRET_ACCESS_KEY_ID')



print(get_trading_days_ytd()[:3])
print(get_trading_days_range(
    start_date=get_5year_ago_date(),
    end_date=today()
)[:3])
print(format_date(today()))
print(get_5year_ago_date())