import os
import dotenv
import time
import argparse
from datetime import datetime

from datafin.utils import (                      # type: ignore
    get_trading_days_ytd,
    get_trading_days_range,
    format_date
)

from datafin import polygon_min_eod_aggs_to_s3                # type: ignore

def process_days(days):
    total_start_time = time.time()
    print(f"Starting processing of {len(days)} days...")

    for day in days:
        formatted_day = format_date(day) 
        day_start_time = time.time()
        print(f"Processing {formatted_day}...")
        try:
            polygon_min_eod_aggs_to_s3(day)
            day_duration = time.time() - day_start_time
            print(f"Completed {formatted_day} in {day_duration:.2f} seconds")
        except Exception as e:
            print(f"Error processing {formatted_day}: {str(e)}")
            continue

    total_duration = time.time() - total_start_time
    print(f"\nTotal processing time: {total_duration:.2f} seconds")
    print(f"Average time per day: {total_duration/len(days):.2f} seconds")

def main():
    parser = argparse.ArgumentParser(description='Process Polygon data for specific months and years')
    parser.add_argument('-y', '--years', nargs='+', type=int, help='Years to process (e.g., 2023 2024)')
    parser.add_argument('-m', '--months', nargs='+', type=int, help='Months to process (1-12)')
    parser.add_argument('--ytd', action='store_true', help='Process year-to-date data')
    
    args = parser.parse_args()

    dotenv.load_dotenv()

    if args.ytd:
        trading_days = get_trading_days_ytd(format_dates=False)
        process_days(trading_days)
    else:
        if not args.years or not args.months:
            print("Error: Both --years and --months are required unless using --ytd")
            return

        for year in args.years:
            for month in args.months:
                if month < 1 or month > 12:
                    print(f"Error: Month {month} is invalid. Must be between 1 and 12.")
                    continue
                
                start_date = f"{year}-{month:02d}-01"
                if month == 12:
                    end_date = f"{year+1}-01-01"
                else:
                    end_date = f"{year}-{month+1:02d}-01"
                
                trading_days = get_trading_days_range(start_date, end_date, format_dates=False)
                month_days = [day for day in trading_days if day.month == month]
                
                if month_days:
                    print(f"\nProcessing {year}-{month:02d}")
                    process_days(month_days)
                else:
                    print(f"No trading days found for {year}-{month:02d}")

if __name__ == "__main__":
    main() 