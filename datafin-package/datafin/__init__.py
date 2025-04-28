"""
DataFin - Financial Data Processing Package
"""

__version__ = "0.3.0"


from .s3_client import S3Client
from .fmp_client import FMPClient
from .polygon_client import PolygonClient

from .utils import (
    APICall,
    get_trading_days_ytd,
    get_trading_days_range,
    format_date,
    today,
    get_5year_ago_date
)

__all__ = [
    'S3Client',
    'FMPClient',
    'PolygonClient',
    'APICall',
    'get_trading_days_ytd',
    'get_trading_days_range',
    'format_date',
    'today',
    'get_5year_ago_date'
]