import os
import dotenv
import time
from datetime import datetime

from datafin.utils import (                      # type: ignore
    get_trading_days_ytd,
    format_date
)

from polygon_to_s3 import polygon_to_s3 

dotenv.load_dotenv()

trading_days = get_trading_days_ytd(format_dates=False)
feb_days = trading_days[trading_days.month == 2]

total_start_time = time.time()
print(f"Starting processing of {len(feb_days)} days...")

for day in feb_days:
    formated_day = format_date(day) 
    day_start_time = time.time()
    print(f"Processing {formated_day}...")
    polygon_to_s3(day)
    day_duration = time.time() - day_start_time
    print(f"Completed {formated_day} in {day_duration:.2f} seconds")

total_duration = time.time() - total_start_time
print(f"\nTotal processing time: {total_duration:.2f} seconds")
print(f"Average time per day: {total_duration/len(feb_days):.2f} seconds")