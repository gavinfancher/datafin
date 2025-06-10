import io
from typing import Dict, Any, Optional
import json

import pandas as pd
import boto3
from botocore.config import Config

class SecretsClient:
    def __init__(
            self, 
            aws_access_key_id: str = None,
            aws_secret_access_key: str = None,
            region_name: Optional[str] = 'us-east-1'
    ) -> None:
        """
        comment here
        """

        self.secrets = boto3.client(
            'secretsmanager',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    
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
        
