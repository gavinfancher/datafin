from typing import Dict, List, Optional, Union, Any

import os
import dotenv
import io
import json
import datetime
from dateutil.relativedelta import relativedelta


import boto3
import pandas as pd
from botocore.config import Config
import requests
from polygon import RESTClient
from sqlalchemy import create_engine, text
import pandas_market_calendars as mcal




class FMPClient:
    def __init__(
        self,
        api_key: str,
    ) -> None:
        
        """
        docs
        """

        self.api_key = api_key
        self.base_url = 'https://financialmodelingprep.com/stable/'

        self.api = APICall(
            base_url=self.base_url,
            api_key=self.api_key
        )


    #######################################################
    #######################################################


    def get_quote(
        self,
        symbol: str
    ) -> Dict[str, Any]:
        """
        docs
        """
        endpoint = 'quote'
        params = {
            'symbol': symbol,
            'apikey': self.api_key
        }

        response = self.api.get(
            endpoint,
            params=params
        )

        return response


    #######################################################


    def get_forex_eod_5min(
        self,
        symbol: str,
        _from: str,
        _to: str
    ) -> Dict[str, Any]:
        """
        docs
        """
        endpoint = 'historical-chart/5min'

        params = {
            'symbol': symbol,
            'from': _from,
            'to': _to,
            'apikey': self.api_key
        }

        response = self.api.get(
            endpoint,
            params=params
        )

        return response










    def test():
        return 'test'
    

class PolygonClient:
    def __init__(
            self,
            api_key: str
    ):
        self.api_key = api_key
        self.client = RESTClient(api_key=self.api_key)


    def get_eod_aggs(
            self,
            date: str,
            symbol: str
    ):
        response = self.client.get_daily_open_close_agg(
            ticker=symbol,
            date=date,
            adjusted="true",
        )

        return response
    

    def get_eod_second_aggs(
            self,
            symbol: str,
            date: str
    ):

        aggs = []
        for a in self.client.list_aggs(
            symbol,
            1,
            "second",
            date,
            date,
            adjusted="true",
            sort="asc",
        ):
            aggs.append(a)

        return aggs


class RDSClient:
    def __init__(
        self,
        host_connection_name: str,
        user: str,
        password: str,
        database: Optional[str] = None,
        port: int = 3306
    ) -> None:
        
        """
        docs
        """

        self.host = host_connection_name
        self.user = user
        self.password = password
        self.database = database
        self.port = port

        self.engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:{self.port}/{self.database if self.database else ''}"
        )
        
        if self.database:
            self.use_database(self.database)


    #######################################################
    #######################################################
        

    def list_databases(self) -> List[str]:
        """
        List all available databases
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("SHOW DATABASES"))
            return [row[0] for row in result]
    

    #######################################################


    def list_tables(self) -> List[str]:
        """
        docs
        """
        if not self.database:
            raise ValueError("No database selected. Use use_database() first.")
            
        with self.engine.connect() as conn:
            result = conn.execute(text("SHOW TABLES"))
            return [row[0] for row in result]
    

    #######################################################


    def use_database(self, database: str) -> None:
        
        """
        docs
        """
        
        self.database = database
        self.engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )
    

    #######################################################


    def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """
        Drop a table from the current database.
        
        Args:
            table_name: Name of the table to drop
            if_exists: If True, adds IF EXISTS clause to prevent errors if table doesn't exist
        """
        if not self.database:
            raise ValueError("No database selected. Use use_database() first.")
            
        if_exists_clause = "IF EXISTS" if if_exists else ""
        with self.engine.connect() as conn:
            conn.execute(text(f"DROP TABLE {if_exists_clause} {table_name}"))
            conn.commit()


    #######################################################
     
       
    def query(self, query: str) -> pd.DataFrame:
        """
        docs
        """
        if not self.database:
            raise ValueError("No database selected. Use use_database() first.")
            
        return pd.read_sql(query, self.engine)


    #######################################################


    def write_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append', index: bool = False) -> None:
        """
        Write a pandas DataFrame to a database table.
        
        Args:
            df: pandas DataFrame to write to the database
            table_name: Name of the table to write to
            if_exists: How to behave if the table already exists
                - 'fail': Raise a ValueError
                - 'replace': Drop the table before inserting new values
                - 'append': Insert new values to the existing table
            index: Whether to write the DataFrame's index as a column
        """
        if not self.database:
            raise ValueError("No database selected. Use use_database() first.")
            
        if if_exists not in ['fail', 'replace', 'append']:
            raise ValueError("if_exists must be one of: 'fail', 'replace', 'append'")
            
        df.to_sql(
            name=table_name,
            con=self.engine,
            if_exists=if_exists,
            index=False,
            chunksize=10000
        )


    #######################################################


    def close(self) -> None:
        """
        docs
        """
        if self.engine:
            self.engine.dispose()


class S3Client:
    def __init__(
            self, 
            bucket_name: Optional[str] = None,
            aws_access_key_id: str = None,
            aws_secret_access_key: str = None,
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
    

class APICall:
    def __init__(
        self,
        base_url: str = None,
        api_key: str = None
    ) -> None:
        
        """
        docs
        """

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

        if self.api_key is None or self.base_url is None:
            raise ValueError('api_key and base_url must be provided')
        

    #######################################################
    #######################################################


    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> Union[Dict[str, Any], str]:
        """
        Make a GET request to the specified endpoint
        
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        params = params or {}
        headers = headers or {}


        url = f'{self.base_url}/{endpoint}'
        response = requests.get(url, params=params)

        if response.status_code != 200:
            return response.raise_for_status()
        else:
            data = response.json()
            if not data:
                return "empty response"
            else:
                return data


                
    def test():
        return 'test'
    

class DateTimeUtils:

    def get_trading_days_ytd(
        less_today: bool = True,
        format_dates: bool = True,
        exchange: str = 'NYSE'
    ) -> List[str]:
        
        """
        docs
        """

        trading_calendar = mcal.get_calendar(exchange)
        
        current_year = datetime.datetime.now().year
        start_date = f'{current_year}-01-01'
        end_date = None

        if less_today:
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            end_date = yesterday.strftime('%Y-%m-%d')
        else:
            end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        
        trading_days = trading_calendar.valid_days(
            start_date=start_date,
            end_date=end_date
        )

        if format_dates:
            return [day.strftime('%Y-%m-%d') for day in trading_days]
        else:
            return trading_days
        

    #######################################################
    #######################################################


    def get_trading_days_range(
        start_date: str,
        end_date: str,
        format_dates: bool = True,
        exchange: str = 'NYSE'
    ) -> List[str]:
        
        """
        docs
        """

        trading_calendar = mcal.get_calendar(exchange)
        
        trading_days = trading_calendar.valid_days(
            start_date=start_date,
            end_date=end_date
        )
        

        if format_dates:
            return [day.strftime('%Y-%m-%d') for day in trading_days]
        else:
            return trading_days


    #######################################################
    #######################################################


    def format_date(date: datetime.date) -> str:
        
        """
        docs
        """
        
        return date.strftime('%Y-%m-%d')


    #######################################################
    #######################################################

    def today(format_date: bool = False) -> str:
        
        """
        docs
        """
        
        if format_date:
            return datetime.datetime.now().strftime('%Y-%m-%d')
        else:
            return datetime.datetime.now()
        

    #######################################################
    #######################################################


    def yesterday(format_date: bool = False) -> str:
        
        """
        docs
        """
        
        yesterday_date = today() - datetime.timedelta(days=1)
        
        if format_date:
            return yesterday_date.strftime('%Y-%m-%d')
        else:
            return yesterday_date


    #######################################################
    #######################################################


    def get_5year_ago_date() -> str:
        
        """
        docs
        """

        today = datetime.datetime.now()
        five_years_ago = today - relativedelta(years=5)
        result = five_years_ago + datetime.timedelta(days=1)
        return result.strftime('%Y-%m-%d')


    #######################################################
    #######################################################


    def string_formating(number: int) -> str:
        
        """
        docs
        """

        if number < 10:
            return f"0{number}"
        else:
            return str(number)
        

    import os
    import dotenv


    import S3Client
    from DateTimeUtils import (
        string_formating,
        format_date
    )


class Functions:
    def polygon_min_eod_aggs_to_s3(date_time_object):

        dotenv.load_dotenv()
        
        #######################################################
        # Credentials
        #######################################################


        s3_bucket = os.getenv('PERSONAL_S3_BUCKET_NAME')
        aws_access_key =  os.getenv('PERSONAL_AWS_ACCESS_KEY_ID')
        aws_secret_key =  os.getenv('PERSONAL_AWS_SECRET_ACCESS_KEY_ID')

        polygon_access_key =  os.getenv('POLYGON_AWS_ACCESS_KEY_ID')
        polygon_access_key_secret_key =  os.getenv('POLYGON_AWS_SECRET_ACCESS_KEY_ID')


        #######################################################
        # Clients
        #######################################################


        my_s3 = S3Client(
            bucket_name=s3_bucket,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name='us-east-1'
        )

        polygon_s3 = S3Client(
            aws_access_key_id=polygon_access_key,
            aws_secret_access_key=polygon_access_key_secret_key,
            is_polygon=True
        )


        date_uf = date_time_object


        date_year = DateTimeUtils.string_formating(date_uf.year)
        date_month = DateTimeUtils.string_formating(date_uf.month)
        date_day = DateTimeUtils.string_formating(date_uf.day)

        

        polygon_minute_aggs_path = f'us_stocks_sip/minute_aggs_v1/{date_year}/{date_month}'
        polygon_file_name = f'{date_year}-{date_month}-{date_day}'


        polygon_s3_df = polygon_s3.get_csv_compressed(
            path=polygon_minute_aggs_path,
            file_name=polygon_file_name
        )


        my_s3_raw_path = f'dev/polygon/equities/min-eod-aggs/raw/year={date_year}/month={date_month}'
        my_s3_file_name = f'raw-{date_year}-{date_month}-{date_day}'


        my_s3.post_parquet(
            data=polygon_s3_df,
            path=my_s3_raw_path,
            file_name=my_s3_file_name
        )