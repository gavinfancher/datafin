from datetime               import datetime, timedelta
from dateutil.relativedelta import relativedelta

import requests
import boto3
import json


class FMPClient:
    def __init__(
            self,
            api_key):
        """
        Initialize a Financial Modeling Prep API client
        
        Args:
            api_key (str): FMP API key
        """
        self.api_key = api_key
        self.base_url = 'https://financialmodelingprep.com/stable/'

    def api_call_get(
            self,
            url: str, 
            params: dict,
            headers: dict = None,
            return_other_than_json: bool = False
    ) -> object:
        """
        DEPS:
            requests
        DEF:
            takes a url and parameters and packages and sends the get requests and returns json or response object
        ARGS:
            url: the url endpoint you want to call
            params: {key: value} pairs that serve as parameters for api
            headers: pass headers as {key: value} pairs for more advance config and authorization
            return_other_than_json: returns the response object rather than the json formatted response which allows for more inspection
        RETURNS:
            list of dicts or reponse object if successful, statuscode if not
        """
        if headers is not None:
            response = requests.get(url, params=params, headers=headers)
            if response.status_code != 200:
                return response.raise_for_status()
            else:
                return response.json()
        else:
            response = requests.get(url, params=params)
            if response.status_code != 200:
                return response.raise_for_status()
            else:
                if response.json() == []:
                    return "empty response"
                else:
                    return response.json()
                
    
    def get_end_of_day_full(
            self,
            symbol,
            _from=None,
            _too=None
    ):
        today = Utils.now()
        time_delta_5_year = today - relativedelta(years=5) + timedelta(days=1)

        function_specific_url = 'historical-price-eod/full'
        url = self.base_url + function_specific_url

        today_param = Utils.format_date_time_object(today)
        time_delta_param = Utils.format_date_time_object(time_delta_5_year)

        params = {
            "symbol": symbol,
            "apikey": self.api_key,
            "from": time_delta_param,
            "to": today_param
        }

        response = self.api_call_get(url, params=params)

        return response





class Utils:
    def format_date_time_object(
            datetime_object: datetime,
            with_time: bool = False
    ) -> str:
        """
        DEPS:
            datetime -- in requirements.txt
        DEF:
            format a datetime object as a string.
        
        ARGS:
            datetime_object: the datetime object to format
            with_time: whether to include the time in the formatted string
        
        RETURNS:
            a formatted date string
        """

        if with_time:
            return datetime_object.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return datetime_object.strftime("%Y-%m-%d")
    
    def time_delta(
            test
    ):
        #time_delta_5_year = today - relativedelta(years=5) + timedelta(days=1)
        pass
        

    def now():
        return datetime.now()
    


class AWSClient:
    def __init__(
            self,
            aws_account_access_key,
            aws_secret_account_access_key
    ):
        self.access_key = aws_account_access_key
        self.secret_key = aws_secret_account_access_key

    def s3_client(
            self,
            region
    ):
        client_instance = boto3.client(
            's3',
            aws_access_key_id = self.access_key,
            aws_secret_access_key = self.secret_key,
            region_name = region
        )
        return client_instance

    def post_json_to_s3(
            self,
            instance,
            data,
            bucket_name,
            file_path,
            file_name,
    ):
        instance.put_object(
            Bucket = bucket_name,
            Key = file_path + '/' + file_name + '.json',
            Body = json.dumps(data),
            ContentType = 'application/json'
        )

    def get_json_from_s3(
            self,
            instance,
            bucket_name,
            file_path,
            file_name,
            raw = False
    ):
        raw_object = instance.get_object(
            Bucket = bucket_name,
            Key = file_path + '/' + file_name + '.json',
        )

        if raw == True:
            return raw_object
        
        else:
            parsed_object = json.loads(raw_object['Body'].read().decode('utf-8'))
            return parsed_object
        

    def get_csv_from_s3(
            self,
            instance,
            bucket_name,
            file_path,
            file_name,
            as_list = False,
            raw = False
    ):
        raw_object = instance.get_object(
            Bucket = bucket_name,
            Key = file_path + '/' + file_name + '.csv',
        )

        if raw == True:
            return raw_object
        
        else:
            csv_data = raw_object['Body'].read().decode('utf-8')
            
            if as_list:
                import csv
                from io import StringIO
                
                csv_list = []
                csv_file = StringIO(csv_data)
                csv_reader = csv.reader(csv_file)
                
                for row in csv_reader:
                    csv_list.append(row)
                    
                return csv_list
            
            return csv_data