import datetime
import pandas_market_calendars as mcal
from typing import List, Optional, Tuple
import pytz


def get_trading_days_ytd(
    less_today: bool = True,
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

    return trading_days.tolist()
    


def get_trading_days_range(
    start_date: str,
    end_date: str,
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
    
    return trading_days


def now() -> datetime:
    """
    docs
    """
    
    return datetime.datetime.now()


def yesterday() -> datetime:
    """
    docs
    """
    
    yesterday_dt = now() - datetime.timedelta(days=1)
    
    return yesterday_dt


def get_5years_ago() -> datetime:
    """
    docs
    """

    today = datetime.datetime.now()
    five_years_ago = today - datetime.timedelta(years=5)
    result_dt = five_years_ago + datetime.timedelta(days=1)
    
    return result_dt


def to_ny_time(dt: datetime) -> datetime:
    """
    docs
    """
    utc_tz = pytz.UTC
    ny_tz = pytz.timezone('America/New_York')
    
    utc_time = dt.astimezone(utc_tz)
    ny_time = utc_time.astimezone(ny_tz)
    
    return ny_time


def string_formating(number: int) -> str:
    """
    docs
    """

    if number < 10:
        return f"0{number}"
    else:
        return str(number)
    

def format_date(dt: datetime) -> str:
    """
    docs
    """
    
    return dt.strftime('%Y-%m-%d')
