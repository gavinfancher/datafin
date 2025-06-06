from dagster import job, op, In, Out

from datafin import S3Client
import pandas as pd
from datetime import datetime

@op(
    out=Out(pd.DataFrame, description="Raw SPY price data")
)
def fetch_spy_data(context) -> pd.DataFrame:
    """Fetch SPY data from Yahoo Finance"""
    context.log.info("Fetching SPY data from Yahoo Finance")
    
    spy = yf.Ticker("SPY")
    # Get last 30 days of data
    data = spy.history(period="1mo")
    
    context.log.info(f"Fetched {len(data)} rows of SPY data")
    return data

@op(
    ins={"raw_data": In(pd.DataFrame)},
    out=Out(pd.DataFrame, description="Processed SPY data with indicators")
)
def process_spy_data(context, raw_data: pd.DataFrame) -> pd.DataFrame:
    """Process SPY data and add technical indicators"""
    context.log.info("Processing SPY data and adding indicators")
    
    # Add simple moving averages
    raw_data['SMA_10'] = raw_data['Close'].rolling(window=10).mean()
    raw_data['SMA_20'] = raw_data['Close'].rolling(window=20).mean()
    
    # Add daily returns
    raw_data['Daily_Return'] = raw_data['Close'].pct_change()
    
    # Add volatility (rolling 10-day std)
    raw_data['Volatility'] = raw_data['Daily_Return'].rolling(window=10).std()
    
    context.log.info("Added technical indicators to SPY data")
    return raw_data

@op(
    ins={"processed_data": In(pd.DataFrame)},
    out=Out(dict, description="SPY analysis summary")
)
def analyze_spy_data(context, processed_data: pd.DataFrame) -> dict:
    """Analyze SPY data and generate summary"""
    context.log.info("Analyzing SPY data")
    
    latest_data = processed_data.iloc[-1]
    
    analysis = {
        "date": latest_data.name.strftime("%Y-%m-%d"),
        "current_price": round(latest_data['Close'], 2),
        "daily_return": round(latest_data['Daily_Return'] * 100, 2),
        "sma_10": round(latest_data['SMA_10'], 2),
        "sma_20": round(latest_data['SMA_20'], 2),
        "volatility": round(latest_data['Volatility'] * 100, 2),
        "above_sma_10": latest_data['Close'] > latest_data['SMA_10'],
        "above_sma_20": latest_data['Close'] > latest_data['SMA_20'],
        "analysis_timestamp": datetime.now().isoformat()
    }
    
    context.log.info(f"SPY Analysis: Price=${analysis['current_price']}, "
                    f"Daily Return={analysis['daily_return']}%")
    
    return analysis

@job
def get_spy_daily_job():
    """Job to fetch, process, and analyze SPY data"""
    raw_data = fetch_spy_data()
    processed_data = process_spy_data(raw_data)
    analysis = analyze_spy_data(processed_data)