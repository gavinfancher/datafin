from dotenv import load_dotenv
import os

import requests
from sqlalchemy import create_engine, text
import pandas as pd

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

load_dotenv()

fmp_api_key = os.getenv('FMP_API_KEY')

db_name = os.getenv('DB_NAME')
db_password = os.getenv('DB_PASSWORD')

mysql_engine = create_engine(f"mysql+mysqlconnector://root:{db_password}@127.0.0.1:3306/{db_name}")
conn = mysql_engine.connect()
list(conn.execute(text("show tables;")))


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
    

def api_call_get(
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

def call_eod(symbol):
    today = datetime.now()
    time_delta_5_year = today - relativedelta(years=5) + timedelta(days=1) ## max amount of data i can get form api


    base_url = "https://financialmodelingprep.com/stable/historical-price-eod/full"

    today_param = format_date_time_object(today)
    time_delta_param = format_date_time_object(time_delta_5_year)


    params = {
        "symbol": symbol,
        "apikey": fmp_api_key,
        "from": time_delta_param,
        "to": today_param
    }

    response = api_call_get(base_url, params=params)

    return response

def transform_and_commit_data(
    response: object
) -> None:
    """
    DEPS:
        pandas, established connection to a sql database, in this case a mysql data base
    DEF:
        transform response json to df, make a primary key transformation and then write to a sql table
    ARGS:
        reponse: response object from an api in json format
    RETURNS:
        nothing, commits to the db or throws an error, the data should be appended if already exists
    """
    df = pd.DataFrame(response)
    df['date_time_id'] = df['symbol'] + '_' + df['date']
    cols = ['date_time_id'] + [col for col in df.columns if col != 'date_time_id']
    df = df[cols]
    df.to_sql(
        name='raw_ingestion_test',
        con=mysql_engine,
        if_exists='append',
        index=False,
        chunksize=1000
    )
    conn.commit()

stocks_to_gather = [
    'AAPL',
    'AMZN',
    'AXP',
    'BLK',
    'BX',
    'COST',
    'CRM',
    'GOOG',
    'GS',
    'JPM',
    'MNST',
    'MSFT',
    'NFLX',
    'PLTR',
    'QQQ',
    'SBUX',
    'SCHD',
    'SG',
    'SOFI',
    'YUM'
]

for s in stocks_to_gather:
    raw_json_reponse = call_eod(s)
    df = transform_and_commit_data(raw_json_reponse)
    print(f'commited {s} to raw_ingestion_test table!')
