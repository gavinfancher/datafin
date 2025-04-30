import os
import dotenv
import time
import argparse
import pandas as pd
from datetime import datetime

from datafin.utils import (                      # type: ignore
    get_trading_days_range,
    format_date,
    string_formating
)

from datafin import PolygonClient                # type: ignore
from datafin import S3Client                     # type: ignore
from datafin import RDSClient                    # type: ignore


#######################################################
# Credentials
#######################################################

fmp_api_key = os.getenv('FMP_API_KEY')

s3_bucket = os.getenv('PERSONAL_S3_BUCKET_NAME')
aws_access_key =  os.getenv('PERSONAL_AWS_ACCESS_KEY_ID')
aws_secret_key =  os.getenv('PERSONAL_AWS_SECRET_ACCESS_KEY_ID')

polygon_api_key = os.getenv('POLYGON_API_KEY')

rds_host_name = os.getenv('RDS_DB_HOST_CONNECTION_NAME')
rds_username = os.getenv('RDS_DB_USERNAME')
rds_password = os.getenv('RDS_DB_PASSWORD')


#######################################################
# Clients
#######################################################

my_s3 = S3Client(
    bucket_name=s3_bucket,
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name='us-east-1'
)

polygon_client = PolygonClient(polygon_api_key)

def fill_missing_seconds(df, date):
    # First, fix the date_time conversion in the incoming dataframe if needed
    if 'date_time' in df.columns and '1969' in str(df['date_time'].iloc[0]):
        # Recreate date_time column properly from timestamp
        df['date_time'] = pd.to_datetime(df['timestamp'], unit='ns').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.tz_localize(None)
    
    # Create a complete range of seconds for the day
    start_time = pd.Timestamp(date + ' 04:00:00')
    end_time = pd.Timestamp(date + ' 19:59:59')
    all_seconds = pd.date_range(start=start_time, end=end_time, freq='s')
    
    # Get unique existing seconds
    existing_seconds = set(df['date_time'])
    
    # Create rows only for missing seconds
    missing_rows = []
    for second in all_seconds:
        if second not in existing_seconds:
            row = {'date_time': second}
            # Add zeros for numeric columns
            for col in ['open', 'high', 'low', 'close', 'volume', 'vwap', 'transactions']:
                if col in df.columns:
                    row[col] = 0
            # Add date
            row['date'] = date
            missing_rows.append(row)
    
    # If there are missing rows, create a DataFrame and append to the original
    if missing_rows:
        missing_df = pd.DataFrame(missing_rows)
        result_df = pd.concat([df, missing_df], ignore_index=True)
        # Sort by date_time to maintain chronological order
        result_df = result_df.sort_values('date_time').reset_index(drop=True)
        return result_df
    else:
        # If no missing rows, return the original DataFrame
        return df

def process_days(days):
    total_start_time = time.time()
    print(f"Starting processing of {len(days)} days...")
    
    df = pd.DataFrame()

    for day in days:
        formatted_day = format_date(day) 
        print(f"Processing {formatted_day}...")
        
        data = polygon_client.get_eod_second_aggs(
            'SPY',
            formatted_day
        )
        
            
        day_df = pd.DataFrame(data)
        day_df['date'] = formatted_day
# In the process_days function, change this line:
    day_df['date_time'] = pd.to_datetime(day_df['timestamp'], unit='ns').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.tz_localize(None)
        

        day_df = fill_missing_seconds(day_df, formatted_day)
        
        df = pd.concat([df, day_df], ignore_index=True)
        print(f"Completed {formatted_day}")

        print(df.head())
        break

    if df['open'].nunique() == 1:
        print("issue!")

    month = string_formating(days[0].month)
    year = string_formating(days[0].year)

    print("\nPosting data to S3...")
    my_s3.post_parquet(
        data=df,
        path=f'dev/polygon/equities/sec-eod-aggs/raw/year={year}/month={month}',
        file_name=f'raw-spy-{year}-{month}'
    )

    print("Done posting to S3!")
    total_duration = time.time() - total_start_time
    print(f"\nTotal processing time: {total_duration:.2f} seconds")
    print(f"Average time per day: {total_duration/len(days):.2f} seconds")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', '--year', type=int, required=True)
    parser.add_argument('-m', '--month', type=int, required=True)
    
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
        process_days(month_days)
    else:
        print(f"No trading days found for {args.year}-{args.month:02d}")

if __name__ == "__main__":
    dotenv.load_dotenv()
    main()
