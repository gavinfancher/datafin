"""
DataFin - Financial Data Processing Package
"""

__version__ = "1.0.0"

from .apis import *
from .aws import *


from .utils import (
    APICall,
    get_trading_days_ytd,
    get_trading_days_range,
    format_date,
    today,
    yesterday,
    get_5year_ago_date,
    string_formating
)



__all__ = [
    'apis'
    'aws'
    'FMPClient',
    'PolygonClient',
    'APICall',
    'get_trading_days_ytd',
    'get_trading_days_range',
    'format_date',
    'today',
    'yesterday',
    'get_5year_ago_date',
    'string_formating'
]