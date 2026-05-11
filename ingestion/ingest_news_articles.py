import os
from datetime import datetime, timezone, timedelta
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

import hashlib
import requests

load_dotenv()

TICKERS = ["AAPL", "MSFT", "GOOGL", "META", "NVDA", "AMZN", "TSLA", "JPM", "V", "JNJ"]
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = "raw"
TABLE = "news_articles"
NEWSAPI_KEY = os.environ["NEWS_API_KEY"]
NEWSAPI_URL = "https://newsapi.org/v2/everything"
BACKFILL_DAYS = 29 #free tier allow 30days

# get watermark 
def get_watermark(bq_client):
    query = f""" 
        SELECT MAX(published_date) AS last_date
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    """
    try: 
        result = bq_client.query(query).result()
        row = list(result)[0]
        return row.last_date
    except Exception:
        return None
    
# pull data 
def pull_news_articles(start_date,end_date):
    all_articles = []

    for ticker in TICKERS:
        response = requests.get(
            NEWSAPI_URL,
            params={
                'q': ticker,
                'language': 'en',
                'sortBy': 'publishedAt',
                'from': start_date.isoformat(),
                'to': end_date.isoformat(),
                'pageSize': 100,
                'apiKey': NEWSAPI_KEY
            }
        ) 

        data = response.json()
        articles = data.get('articles', [])
        print(f'{ticker}: {len(articles)} articles found')
        for article in articles:
            article['symbol'] = ticker
        all_articles.extend(articles)

    return all_articles

# convert to dataframe
def convert_to_df(articles):
    rows = []
    now = datetime.now(tz=timezone.utc)

    for article in articles:
        url = article.get('url','')
        if not url:
            continue

        article_id = hashlib.md5(url.encode()).hexdigest()
        published_at_raw = article.get('publishedAt','')
        try:
            published_at = datetime.fromisoformat(
                published_at_raw.replace('Z','+00:00')
            )
        except ValueError:
            continue
        rows.append({
            'article_id': article_id,
            'symbol': article.get('symbol',''),
            'title': article.get('title',''),
            'description': article.get('description',''),
            'url': url,
            'source_name': article.get('source',{}).get('name',''),
            'published_at': published_at,
            'published_date': published_at.date(),
            '_loaded_at': now
        })
    
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=['article_id'])

    return df

# write to big query
def write_to_bigquery(bq_client,df):
    if df.empty:
        print('No rows to write. Skipping BigQuery Load.')
        return
    
    table_id = f'{PROJECT_ID}.{DATASET}.{TABLE}'
    job_config = bigquery.LoadJobConfig(
        write_disposition = 'WRITE_APPEND'
    )
    job = bq_client.load_table_from_dataframe(df,table_id,job_config)
    job.result()
    print(f'Wrote {len(df)} rows to {table_id}')

# main
def main():
    bq_client = bigquery.Client(project = PROJECT_ID)
    
    watermark = get_watermark(bq_client)
    if watermark is None:
        start_date = datetime.now().date() - timedelta(days=BACKFILL_DAYS)
        print(f'First run - Collect news from the last 29 days, starting on {start_date}')
    else:
        start_date = watermark + timedelta(days=1)
        print(f'Latest data in the set is from {watermark}')
        print(f'Incremental load new data- from {start_date}')
    end_date = datetime.now().date() - timedelta(days=1)

    if start_date > end_date:
        print(f'Already up to date. Wait for tomorrow to load today news.')
        return
    
    articles = pull_news_articles(start_date,end_date)
    df = convert_to_df(articles)

    print(df.head())
    print(f'Total rows: {len(df)}')

    write_to_bigquery(bq_client,df)

if __name__ == '__main__':
    main()
