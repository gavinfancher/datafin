import os

import dotenv
dotenv.load_dotenv()

from datafin.aws import SecretsClient                      #type: ignore



_env_PERSONAL_AWS_KEY = os.getenv('PERSONAL_AWS_ACCESS_KEY')
_env_PERSONAL_AWS_SECRET_KEY = os.getenv('PERSONAL_AWS_SECRET_ACCESS_KEY')


secrets = SecretsClient(
    aws_access_key=_env_PERSONAL_AWS_KEY,
    aws_secret_access_key=_env_PERSONAL_AWS_SECRET_KEY,
)


fmp_api_key = secrets.get_fmp_api_key()
polygon_api_key = secrets.get_polygon_api_key()
print(fmp_api_key, polygon_api_key)