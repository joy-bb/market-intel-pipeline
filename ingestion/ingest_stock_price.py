import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import pandas as pd
from google.cloud import bigquery
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

load_dotenv()

TICKERS = ["AAPL", "MSFT", "GOOGL", "META", "NVDA", "AMZN", "TSLA", "JPM", "V", "JNJ"]
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = "raw"
TABLE = "stock_prices"

# Get watermark 
def get_watermark(bq_client):
    query = f"""
        SELECT MAX(bar_date) AS last_date
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    """
    try: 
        result = bq_client.query(query).result()
        row = list(result)[0]
        return row.last_date
    except Exception:
        return None
    
# Get data from Alpaca 
def pull_bar(alpaca_client, start_date, end_date):
    request = StockBarsRequest(
        symbol_or_symbols = TICKERS,
        timeframe = TimeFrame.Day,
        start = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc),
        end = datetime(end_date.year, end_date.month, end_date.day, tzinfo=timezone.utc),
        feed = 'iex'
    )
    return alpaca_client.get_stock_bars(request)

# Convert to DataFrame
def convert_bars_to_df(bar_set):
    rows = []
    now = datetime.now(tz=timezone.utc)
    
    for symbol, bars in bar_set.data.items():
        for bar in bars:
            rows.append({
                'symbol': bar.symbol,
                'bar_date': bar.timestamp.date(),
                'open': float(bar.open),
                'high': float(bar.high),
                'low': float(bar.low),
                'close': float(bar.close),
                'volume': int(bar.volume),
                'vwap': float(bar.vwap),
                'trade_count': int(bar.trade_count),
                '_loaded_at': now
            })
    return pd.DataFrame(rows)

# Write to BigQuery
def write_to_bigquery(bq_client,df):
    if df.empty:
        print("No rows to write. Skipping BigQuery load.")
        return
    
    table_id = f'{PROJECT_ID}.{DATASET}.{TABLE}'
    job_config = bigquery.LoadJobConfig(
        write_disposition = 'WRITE_APPEND'
    )
    job = bq_client.load_table_from_dataframe(df,table_id,job_config)
    job.result()
    print(f'Wrote {len(df)} rows to {table_id}')

# Main logic
def main():
    bq_client = bigquery.Client(project = PROJECT_ID)
    alpaca_client = StockHistoricalDataClient(
        api_key=os.environ["ALPACA_API_KEY"],
        secret_key=os.environ["ALPACA_SECRET_KEY"],
    )

    watermark = get_watermark(bq_client)
    # setting end_date to be 1 day before
    end_date = datetime.now().date() - timedelta(days=1)

    if watermark is None:
        start_date = datetime(2024,1,1).date()
        print('First run - backfiling from 01-01-2024')
    else:
        start_date = watermark + timedelta(days=1)
        print(f"Last date in dataset: {watermark}")
        print(f"Incremental run — attempting {start_date} to {end_date}")
    
    if start_date>end_date:
        print(f'The most updated data will be on 1 trading day before. Already up to date for {watermark}. Nothing to load. For today data, need to wait for tomorrow to load')
        return
    
    bar_set = pull_bar(alpaca_client,start_date,end_date)
    df = convert_bars_to_df(bar_set)

    print(df.head())
    print(f'Total rows: {len(df)}')

    write_to_bigquery(bq_client,df)

if __name__ == '__main__':
    main()

