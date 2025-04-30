import os
import dotenv
import time
import argparse
import pandas as pd
from datetime import datetime

from datafin.utils import (                      # type: ignore
    get_trading_days_ytd,
    get_trading_days_range,
    format_date
)

from datafin import PolygonClient                # type: ignore
from datafin import polygon_min_eod_aggs_to_s3   # type: ignore

def process_days(days, polygon_client):
    total_start_time = time.time()
    print(f"Starting processing of {len(days)} days...")
    
    all_second_aggs = []
    
    for day in days:
        formatted_day = format_date(day) 
        day_start_time = time.time()
        print(f"Processing {formatted_day}...")
        try:
            # Get second aggregates for SPY
            data = polygon_client.get_eod_second_aggs(
                formatted_day,  # date first
                'SPY'          # symbol second
            )
            
            # Convert to DataFrame and add date column
            df = pd.DataFrame(data)
            df['date'] = formatted_day
            
            all_second_aggs.append(df)
            
            day_duration = time.time() - day_start_time
            print(f"Completed {formatted_day} in {day_duration:.2f} seconds")
        except Exception as e:
            print(f"Error processing {formatted_day}: {str(e)}")
            continue
    
    if all_second_aggs:
        # Combine all DataFrames
        combined_df = pd.concat(all_second_aggs, ignore_index=True)
        
        # Convert timestamp to datetime
        combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'], unit='ms')
        
        # Resample to 1-minute intervals
        combined_df.set_index('timestamp', inplace=True)
        minute_df = combined_df.resample('1T').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'vwap': 'mean'
        }).reset_index()
        
        # Add date column if not present
        if 'date' not in minute_df.columns:
            minute_df['date'] = minute_df['timestamp'].dt.strftime('%Y-%m-%d')
        
        # Post to S3
        print("\nPosting minute aggregates to S3...")
        polygon_min_eod_aggs_to_s3(minute_df)
        print("Done posting to S3!")
    
    total_duration = time.time() - total_start_time
    print(f"\nTotal processing time: {total_duration:.2f} seconds")
    print(f"Average time per day: {total_duration/len(days):.2f} seconds")

def main():
    parser = argparse.ArgumentParser(description='Process SPY second aggregates for specific months and years')
    parser.add_argument('-y', '--year', type=int, required=True, help='Year to process (e.g., 2024)')
    parser.add_argument('-m', '--month', type=int, required=True, help='Month to process (1-12)')
    
    args = parser.parse_args()

    if args.month < 1 or args.month > 12:
        print("Error: Month must be between 1 and 12.")
        return

    start_date = f"{args.year}-{args.month:02d}-01"
    if args.month == 12:
        end_date = f"{args.year+1}-01-01"
    else:
        end_date = f"{args.year}-{args.month+1:02d}-01"
    
    trading_days = get_trading_days_range(start_date, end_date, format_dates=False)
    month_days = [day for day in trading_days if day.month == args.month]
    
    if month_days:
        print(f"\nProcessing {args.year}-{args.month:02d}")
        process_days(month_days, polygon_client)
    else:
        print(f"No trading days found for {args.year}-{args.month:02d}")

if __name__ == "__main__":
    main()
