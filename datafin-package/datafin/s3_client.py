import io
from typing import Dict, Any, Optional
import json

import boto3
import pandas as pd
from botocore.config import Config

class S3Client:
    def __init__(
            self, 
            bucket_name: str = None,
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None,
            region_name: Optional[str] = None,
            is_polygon: bool = False
    ) -> None:
        """
        Initialize S3 client with optional credentials.
        If no credentials are provided, it will use the default AWS credentials.
        """
        self.bucket_name = bucket_name
        self.s3 = None

        config = Config(
            max_pool_connections=50
        )

        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

        if is_polygon:
            self.bucket_name = 'flatfiles'
            self.s3 = session.client(
                's3',
                endpoint_url='https://files.polygon.io',
                config=Config(
                    signature_version='s3v4',
                    max_pool_connections=50,
                )
            )
        else: 
            self.s3 = session.client('s3', config=config)

    
    #######################################################
    #######################################################


    def get_json(
        self,
        path: str,
        file_name: str
    ) -> Dict[str, Any]:
        
        """
        docs
        """

        response = self.s3.get_object(
            Bucket = self.bucket_name,
            Key = path + '/' + file_name + '.json'
        )
        return json.loads(response['Body'].read().decode('utf-8'))
    

    #######################################################


    def post_json(
        self,
        data: Dict,
        path: str,
        file_name: str
    ) -> None:
        
        """
        docs
        """

        data = json.dumps(data)
        self.s3.put_object(
            Bucket = self.bucket_name,
            Key = path + '/' + file_name + '.json',
            Body = json.dumps(data),
            ContentType = 'application/json'
        )


    #######################################################


    def get_parquet(
        self,
        path: str,
        file_name: str,
    ) -> pd.DataFrame:
        
        """
        docs
        """
        
        key = path + '/' + file_name + '.parquet'
        
        response = self.s3.get_object(
            Bucket=self.bucket_name,
            Key=key
        )
        
        buffer = io.BytesIO(response['Body'].read())
        
        return pd.read_parquet(
            buffer,
            engine='pyarrow',
            use_threads=True,
            memory_map=True,
            coerce_int96_timestamp_unit='ns'
        )
                

    #######################################################


    def post_parquet(
        self,
        data: pd.DataFrame,
        path: str,
        file_name: str
    ) -> None:
        
        """
        docs
        """

        buffer = io.BytesIO()
        data.to_parquet(buffer, index=False)
        buffer.seek(0)

        self.s3.put_object(
            Bucket = self.bucket_name,
            Key = path + '/' + file_name + '.parquet',
            Body = buffer.getvalue(),
            ContentType = 'application/parquet'
        )


    #######################################################
    

    def get_csv_compressed(
            self,
            path: str,
            file_name: str
        ) -> pd.DataFrame:
            
        """
        docs
        """
        
        response = self.s3.get_object(
            Bucket = self.bucket_name,
            Key = path + '/' + file_name + '.csv.gz'
        )

        return pd.read_csv(io.BytesIO(response['Body'].read()), compression='gzip')