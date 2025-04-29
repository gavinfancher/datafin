"""
DataFin - Financial Data Processing Package
"""

__version__ = "0.3.0"


from .s3_client import S3Client
from .fmp_client import FMPClient
from .polygon_client import PolygonClient
from .rds_client import RDSClient

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
    'S3Client',
    'FMPClient',
    'PolygonClient',
    'RDSClient',
    'APICall',
    'get_trading_days_ytd',
    'get_trading_days_range',
    'format_date',
    'today',
    'yesterday'
    'get_5year_ago_date',
    'string_formating'
]