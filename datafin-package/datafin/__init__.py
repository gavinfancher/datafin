"""
DataFin - Financial Data Processing Package
"""

__version__ = "0.5.0"


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

from .etl import (
    polygon_min_eod_aggs_to_s3
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
    'yesterday',
    'get_5year_ago_date',
    'string_formating',
    'polygon_min_eod_aggs_to_s3',
    'polygon_sec_eod_aggs_to_s3'
]