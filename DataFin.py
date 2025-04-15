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

    
    def get_eod(
            self,
            symbol,
            _from = None,
            _too = None,
            full = True
    ):

        if full is not True:
            function_specific_endpoint = 'historical-price-eod/light'
            url = self.base_url + function_specific_endpoint

            to_param = _Utils.format_date_time_object(_Utils.today())
            from_param = _Utils.format_date_time_object(_Utils.five_year_lag())
        
            params = {
                "symbol": symbol,
                "apikey": self.api_key,
                "from": from_param,
                "to": to_param
            }

            response = self.api_call_get(url, params=params)
            return response
        
        else:
            function_specific_endpoint = 'historical-price-eod/full'
            url = self.base_url + function_specific_endpoint

            to_param = _Utils.format_date_time_object(_Utils.today())
            from_param = _Utils.format_date_time_object(_Utils.five_year_lag())
        
            params = {
                "symbol": symbol,
                "apikey": self.api_key,
                "from": from_param,
                "to": to_param
            }

            response = self.api_call_get(url, params=params)
            return response
    
    def get_five_min(
            self,
            symbol,
            _from = None,
            _to = None
    ):
        function_specific_endpoint = 'historical-chart/5min'
        url = self.base_url + function_specific_endpoint

        from_param = _Utils.format_date_time_object(_Utils.today())
        to_param = _Utils.format_date_time_object(_Utils.today())

        params = {
            "symbol": symbol,
            "apikey": self.api_key,
            "from": from_param,
            "to": to_param
        }

        response = self.api_call_get(url, params=params)
        return response


    def get_live_quote(
            self,
            symbol,
            short = True
    ):
        if short is not True:
            function_specific_url = 'quote?'
            url = self.base_url + function_specific_url

            params = {
                "symbol": symbol,
                "apikey": self.api_key,
            }

            response = self.api_call_get(url, params=params)
            return response
        
        else:
            function_specific_url = 'quote-short?'
            url = self.base_url + function_specific_url

            params = {
                "symbol": symbol,
                "apikey": self.api_key,
            }

            response = self.api_call_get(url, params=params)
            return response
    

    def get_eod_forex(
            self,
            pair,
            _from = None,
            _too = None,
            full = True
    ):
        if full is not True:
            function_specific_endpoint = 'historical-price-eod/light'
            url = self.base_url + function_specific_endpoint

            from_param = _Utils.format_date_time_object(_Utils.five_year_lag())
            to_param = _Utils.format_date_time_object(_Utils.today())

            params = {
                "symbol": pair,
                "apikey": self.api_key,
                "from": from_param,
                "to": to_param
            }

            response = self.api_call_get(url, params=params)
            return response
        
        else:

            function_specific_endpoint = 'historical-price-eod/full'
            url = self.base_url + function_specific_endpoint

            from_param = _Utils.format_date_time_object(_Utils.five_year_lag())
            to_param = _Utils.format_date_time_object(_Utils.today())

            params = {
                "symbol": pair,
                "apikey": self.api_key,
                "from": from_param,
                "to": to_param
            }

            response = self.api_call_get(url, params=params)
            return response
        
    def get_five_min_forex(
            self,
            pair,
            _from = None,
            _to = None
    ):
        function_specific_endpoint = 'historical-chart/5min'
        url = self.base_url + function_specific_endpoint

        from_param = _Utils.format_date_time_object(_Utils.today())
        to_param = _Utils.format_date_time_object(_Utils.today())

        params = {
            "symbol": pair,
            "apikey": self.api_key,
            "from": from_param,
            "to": to_param
        }

        response = self.api_call_get(url, params=params)
        return response
    


###########################################################



class S3Client:
    def __init__(
            self,
            aws_account_access_key,
            aws_secret_account_access_key,
            region,
            bucket_name
    ):
        self.access_key = aws_account_access_key
        self.secret_key = aws_secret_account_access_key
        self.region = region
        self.bucket_name = bucket_name
        self.client_instance = boto3.client(
            's3',
            aws_access_key_id = self.access_key,
            aws_secret_access_key = self.secret_key,
            region_name = self.region
        )

    def json_post(
            self,
            data,
            file_path,
            file_name,
    ):
        self.client_instance.put_object(
            Bucket = self.bucket_name,
            Key = file_path + '/' + file_name + '.json',
            Body = json.dumps(data),
            ContentType = 'application/json'
        )

    def json_get(
            self,
            file_path,
            file_name,
            raw = False
    ):
        raw_object = self.client_instance.get_object(
            Bucket = self.bucket_name,
            Key = file_path + '/' + file_name + '.json',
        )

        if raw == True:
            return raw_object
        
        else:
            parsed_object = json.loads(raw_object['Body'].read().decode('utf-8'))
            return parsed_object
        


###########################################################



class _Utils:
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
    

    def five_year_lag():
        time_delta_5_year = _Utils.today() - relativedelta(years=5) + timedelta(days=1)
        return time_delta_5_year
        

    def today():
        return datetime.now()