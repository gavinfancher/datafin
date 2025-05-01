import os
import dotenv
import time
import argparse
import datetime
import pytz

import logging
import os


log_directory = "./logs/" 
os.makedirs(log_directory, exist_ok=True)

session_id = f"session_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.getpid()}"

logging.basicConfig(
    level=logging.INFO,
    format=f'%(asctime)s - {session_id} - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{log_directory}/grab-yesterday-unified.log"),
    ]
)

logger = logging.getLogger(__name__)


from datafin.utils import (                                     # type: ignore
    get_trading_days_ytd,
)

from datafin import polygon_min_eod_aggs_to_s3                  # type: ignore

ny_tz = pytz.timezone('America/New_York')
now_ny = datetime.datetime.now(ny_tz)
yesterday_ny = now_ny - datetime.timedelta(days=1)

last_trading_day = get_trading_days_ytd()[-1]

yesterday_formatted = yesterday_ny.strftime('%Y-%m-%d')

if yesterday_formatted == last_trading_day:
    logger.info("starting to write to s3")
    polygon_min_eod_aggs_to_s3(yesterday_ny)
    

logger.info("finished writing to s3")