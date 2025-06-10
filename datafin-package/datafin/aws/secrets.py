import io
from typing import Dict, Any, Optional
import json

import boto3
import dotenv
import os

dotenv.load_dotenv()

class SecretsClient:
    def __init__(
            self, 
            aws_access_key: str = os.getenv('PERSONAL_AWS_ACCESS_KEY'),
            aws_secret_access_key: str = os.getenv('PERSONAL_AWS_SECRET_ACCESS_KEY'),
            region_name: Optional[str] = 'us-east-1'
    ) -> None:
        """
        comment here
        """

        self.secrets = boto3.client(
            'secretsmanager',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key

    
    #######################################################
    #######################################################

    def get_secret(
            self,
            secret_id: str,
            secret_string: str = None
    ):
        """
        docs
        """
        
        secret_raw = self.secrets.get_secret_value(
            SecretId = secret_id
        )

        if secret_string:
            value = json.loads(secret_raw['SecretString'])[secret_string]
            return value

        return secret_raw
    

    def get_polygon_api_key(
            self,
            raw: bool = False
    ):

        raw_secret = self.secrets.get_secret_value(
            SecretId = 'apis/polygon'
        )

        if raw:
            return raw_secret
        
        value = json.loads(raw_secret['SecretString'])['polygon_api_key']
        return value
        
    def get_polygon_aws_key(
            self,
            raw: bool = False
    ):

        raw_secret = self.secrets.get_secret_value(
            SecretId = 'aws/polygon_key'
        )

        if raw:
            return raw_secret
        
        value = json.loads(raw_secret['SecretString'])['polygon_aws_key']
        return value
    
    def get_fmp_api_key(
            self,
            raw: bool = False
    ):

        raw_secret = self.secrets.get_secret_value(
            SecretId = 'apis/fmp'
        )

        if raw:
            return raw_secret
        
        value = json.loads(raw_secret['SecretString'])['fmp_api_key']
        return value
        

    def get_bucket_name(
            self,
            raw: bool = False
    ):
        raw_secret = self.secrets.get_secret_value(
            SecretId = 'aws/datafin_s3_bucket'
        )

        if raw:
            return raw_secret
        
        value = json.loads(raw_secret['SecretString'])['datafin_s3_bucket_name']
        return value