import os
import dotenv
import time
from datetime import datetime

from datafin.utils import (                      # type: ignore
    get_trading_days_ytd
)

from polygon_to_s3 import polygon_to_s3 

dotenv.load_dotenv()

trading_days = get_trading_days_ytd(format_dates=False)
current_month = datetime.now().month
filtered_days = trading_days[trading_days.month == current_month]

for day in filtered_days:
    polygon_to_s3(day)