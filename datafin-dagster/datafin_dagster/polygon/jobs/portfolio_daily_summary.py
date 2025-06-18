from dagster import job, op, schedule, Definitions, RunRequest

import pandas as pd

from datafin.aws import S3Client                           #type: ignore
from datafin.apis import PolygonClient                     #type: ignore
from datafin.utils import GmailClient                      #type: ignore
from datafin.utils import (                                #type: ignore
    now,
    to_ny_time,
    string_formating,
    get_ny_timestamp_for_today_time_range,
    is_today_a_trading_day
)                                

from datafin_dagster.resources.credentials import SecretsResource



@op
def portfolio_list(
        secrets: SecretsResource
) -> list:
    
    s3 = S3Client(
        bucket_name = secrets.client.get_bucket_name(),
        aws_access_key_id = secrets.client.aws_access_key,
        aws_secret_access_key = secrets.client.aws_secret_access_key,
        region_name = 'us-east-1'
    )

    raw_json_list = s3.get_json(
        path = 'v1/reference',
        file_name = 'portfolio'
    )

    cleaned_list = list(raw_json_list['data'])
    return cleaned_list


@op
def portfolio_combined_df(
        input_list,
        secrets: SecretsResource
) -> pd.DataFrame:
    
    pg = PolygonClient(
        api_key = secrets.client.get_polygon_api_key()
    )

    timestamps = get_ny_timestamp_for_today_time_range(
        _from = (9, 30, 0),
        _to = (16, 0, 0)
    )

    df_start = pd.DataFrame()

    for symbol in input_list:
        aggs = pg.get_aggs(
            symbol = symbol,
            multiplier = 15,
            unit = 'minute',
            _from = timestamps[0],
            _to = timestamps[1]
        )
        df = pd.DataFrame(aggs)
        df['symbol'] = symbol
        df_start = pd.concat([df_start, df], ignore_index=True)
    return df_start


@op
def portfolio_with_previous_close_df(
        input_df: pd.DataFrame,
        secrets: SecretsResource
) -> pd.DataFrame:

    pg = PolygonClient(
            api_key = secrets.client.get_polygon_api_key()
        )

    symbols_from_df = input_df['symbol'].unique()

    previous_closes = {}

    for symbol in symbols_from_df:
        previous_close = pg.get_previous_close_agg(symbol)[0].close
        previous_closes[symbol] = previous_close

    input_df['previous_close'] = input_df['symbol'].map(previous_closes)

    return input_df

@op
def portfolio_cleaned_df(
        input_df: pd.DataFrame
) -> pd.DataFrame:
    
    input_df = input_df.drop(columns=['otc'])
    input_df['datetime_ny'] = pd.to_datetime(input_df['timestamp'], unit='ms', utc=True).dt.tz_convert('America/New_York')
    
    input_df['perf_from_prev_close_nom'] = input_df['close'] - input_df['previous_close']
    input_df['perf_from_prev_close_per'] = ((input_df['close'] - input_df['previous_close']) / input_df['previous_close']) * 100
    input_df = input_df.reset_index(drop=True)
    
    return input_df


# @op
# def portfolio_chart_prep_df(
#         input_df: pd.DataFrame
#         # secrets: SecretsResource
# ) -> pd.DataFrame:
    
#     sorted_df = input_df.sort_values(['symbol', 'datetime_ny'])
#     latest_closes_per_symbol = sorted_df.groupby('symbol').last().reset_index()

#     performance_df_for_graph = sorted_df[['symbol', 'open', 'high', 'low', 'close', 'datetime_ny']]

#     return performance_df_for_graph


@op
def portfolio_data_prep_df(
        input_df: pd.DataFrame
) -> pd.DataFrame:
    
    sorted_df = input_df.sort_values(['symbol', 'datetime_ny'])
    latest_closes_per_symbol = sorted_df.groupby('symbol').last().reset_index()

    perfomance_df_for_share = latest_closes_per_symbol[['symbol', 'close', 'perf_from_prev_close_nom', 'perf_from_prev_close_per']]

    return perfomance_df_for_share


@op
def portfolio_text_for_email(
        input_df: pd.DataFrame    
) -> str:
    
    performance_lines = []
    
    for _, row in input_df.iterrows():
        symbol = row['symbol']
        price = row['close']
        pct_change = row['perf_from_prev_close_per']
        nom_change = row['perf_from_prev_close_nom']
        
        pct_sign = "+" if pct_change >= 0 else ""
        pct_formatted = f"{pct_sign}{pct_change:.2f}%"
        
        if nom_change >= 0:
            nom_formatted = f"+${nom_change:.2f}"
        else:
            nom_formatted = f"-${abs(nom_change):.2f}"
        
        line = f"{symbol:<6} ........ {price:>8.2f} ........ {pct_formatted:>7} ........ {nom_formatted:>8}"
        performance_lines.append(line)
    
    return_string = "\n".join(performance_lines)
    return return_string

@op
def portfolio_sent_email(
        input_string: str,
        secrets: SecretsResource
) -> None:
    
    gmail = GmailClient(
        sender_email = secrets.client.get_gmail_address(),
        app_password = secrets.client.get_gmail_app_password()
    )

    gmail.send_email(
        to = secrets.client.get_gmail_send_to_address(),
        subject = "today's portfolio performance",
        text = input_string
    )






@job
def portfolio_daily_summary_job():
    portfolio_sent_email(
        portfolio_text_for_email(
            portfolio_data_prep_df(
                portfolio_cleaned_df(
                    portfolio_with_previous_close_df(
                        portfolio_combined_df(
                            portfolio_list()
                        )
                    )
                )
            )
        )
    )

@schedule(
    job = portfolio_daily_summary_job,
    cron_schedule = '20 16 * * *',
    execution_timezone ='America/New_York'
)
def portfolio_daily_summary_schedule():
    """
    comment here
    """
    if is_today_a_trading_day():
        return [RunRequest(run_key=None)]
    else:
        return []


portfolio_daily_summary_definition = Definitions(
    jobs=[portfolio_daily_summary_job],
    schedules=[portfolio_daily_summary_schedule]
)