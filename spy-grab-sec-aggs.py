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


pd.set_option('future.no_silent_downcasting', True)

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

rds = RDSClient(
    host_connection_name=rds_host_name,
    user=rds_username,
    password=rds_password,
    database='datafin_test_1'
)

polygon_client = PolygonClient(polygon_api_key)

def dt_from_timestamp(
        df: pd.DataFrame,
        from_col: str, 
        to_col: str
    ) -> pd.DataFrame:
        
    df[to_col] = pd.to_datetime(df[from_col], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/New_York').dt.tz_localize(None)

    return df



def fill_missing_seconds(
        date,
        df
):

    start_time = pd.Timestamp(date + ' 04:00:00')
    end_time = pd.Timestamp(date + ' 19:59:59')
    all_seconds_df = pd.DataFrame(pd.date_range(start=start_time, end=end_time, freq='s'), columns=['date_time'])

    merge_df = pd.merge(all_seconds_df, df, on='date_time', how='left')


    columns_to_fill = [col for col in merge_df.columns if col != 'timestamp']
    merge_df[columns_to_fill] = merge_df[columns_to_fill].fillna(0).infer_objects(copy=False)


    merge_df['timestamp'] = merge_df.apply(
        lambda row: pd.Timestamp(row['date_time']).timestamp() if pd.isna(row['timestamp']) else row['timestamp'], 
        axis=1
    )

    return merge_df



def process_days(days):
    total_start_time = time.time()
    
    total_df = pd.DataFrame()

    for day in days:
        formatted_day = format_date(day) 
        
        data = polygon_client.get_eod_second_aggs(
            'SPY',
            formatted_day
        )
        
        temp_df = pd.DataFrame(data)

        dt_df = dt_from_timestamp(
            temp_df,
            'timestamp',
            'date_time'
        )

        
        filled_df  = fill_missing_seconds(
             formatted_day,
             dt_df
        )
        
        total_df = pd.concat([total_df, filled_df], ignore_index=True)
        print(f"Completed {formatted_day}")



    month = string_formating(days[0].month)
    year = string_formating(days[0].year)

    print("\nPosting data to S3...")
    my_s3.post_parquet(
        data=total_df,
        path=f'dev/polygon/equities/sec-eod-aggs/raw/year={year}/month={month}',
        file_name=f'raw-spy-{year}-{month}'
    )
    print("Done posting to S3!")

    print("\nPosting data to RDS...")
    rds.write_dataframe(
        df=total_df,
        table_name='spy_sec_eod_aggs',
        if_exists='append',
    )
    print("Done posting to RDS!")

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
