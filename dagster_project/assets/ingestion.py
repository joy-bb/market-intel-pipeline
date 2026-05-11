from dagster_project.constants import PROJECT_ID,ALPACA_API_KEY,ALPACA_SECRET_KEY,BACKFILL_DAYS

from datetime import datetime, timedelta

from dagster import asset, get_dagster_logger

from google.cloud import bigquery

from alpaca.data.historical import StockHistoricalDataClient

from ingestion.ingest_stock_price import (
    get_watermark as get_stock_watermark,
    pull_bar,
    convert_bars_to_df,
    write_to_bigquery as write_stock_to_bigquery
)

from ingestion.ingest_news_articles import(
    get_watermark as get_news_watermark,
    pull_news_articles,
    convert_to_df,
    write_to_bigquery as write_news_to_bigquery
)

@asset
def raw_stock_prices():
    log = get_dagster_logger()
    bq_client = bigquery.Client(project = PROJECT_ID)
    alpaca_client = StockHistoricalDataClient(
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_SECRET_KEY,
    )

    watermark = get_stock_watermark(bq_client)
    # setting end_date to be 1 day before
    end_date = datetime.now().date() - timedelta(days=1)

    if watermark is None:
        start_date = datetime(2024,1,1).date()
        log.info('First run - backfiling from 01-01-2024')
    else:
        start_date = watermark + timedelta(days=1)
        log.info(f"Last date in dataset: {watermark}")
        log.info(f"Incremental run — attempting {start_date} to {end_date}")
    
    if start_date>end_date:
        log.info(f'The most updated data will be on 1 trading day before. Already up to date for {watermark}. Nothing to load. For today data, need to wait for tomorrow to load')
        return
    
    bar_set = pull_bar(alpaca_client,start_date,end_date)
    df = convert_bars_to_df(bar_set)

    log.info(df.head())
    log.info(f'Total rows: {len(df)}')

    write_stock_to_bigquery(bq_client,df)

@asset
def raw_news_articles():
    log = get_dagster_logger()
    bq_client = bigquery.Client(project = PROJECT_ID)
    
    watermark = get_news_watermark(bq_client)
    if watermark is None:
        start_date = datetime.now().date() - timedelta(days=BACKFILL_DAYS)
        log.info(f'First run - Collect news from the last 29 days, starting on {start_date}')
    else:
        start_date = watermark + timedelta(days=1)
        log.info(f'Latest data in the set is from {watermark}')
        log.info(f'Incremental load new data- from {start_date}')
    end_date = datetime.now().date() - timedelta(days=1)

    if start_date > end_date:
        log.info(f'Already up to date. Wait for tomorrow to load today news.')
        return
    
    articles = pull_news_articles(start_date,end_date)
    df = convert_to_df(articles)

    log.info(f'Total rows: {len(df)}')

    write_news_to_bigquery(bq_client,df)