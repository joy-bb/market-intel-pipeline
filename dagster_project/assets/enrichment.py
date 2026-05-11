from dagster_project.constants import PROJECT_ID, DATASET, TARGET_TABLE, GROQ_API_KEY

from dagster import asset, get_dagster_logger

import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery

from groq import Groq

from ingestion.enrich_news_sentiment import (
    classify_sentiment,
    get_unprocessed_articles
)

@asset(deps=["raw_news_articles"])
def raw_news_sentiment():
    log = get_dagster_logger()
    bq_client = bigquery.Client(project = PROJECT_ID)
    df = get_unprocessed_articles(bq_client)
    
    groq_client = Groq(api_key = GROQ_API_KEY)
    now = datetime.now(tz=timezone.utc)
    score_map = {"positive": 1, "neutral": 0, "negative": -1}
    total_rows = 0
    batch_run = 0
    while not df.empty:      
        sentiments = []
        for _, row in df.iterrows():
            title = row["title"]
            description = row["description"]
            sentiment = classify_sentiment(groq_client,title,description)
            
            sentiments.append({
                'article_id': row["article_id"],
                'sentiment': sentiment["sentiment"],
                'confidence': sentiment["confidence"],
                'sentiment_score': score_map[sentiment["sentiment"]],
                '_enriched_at': now
            })        
        result = pd.DataFrame(sentiments)
        
        # write to target table on bigquery
        job_config = bigquery.LoadJobConfig(
            write_disposition = 'WRITE_APPEND'
        )
        table_id = f'{PROJECT_ID}.{DATASET}.{TARGET_TABLE}'
        job = bq_client.load_table_from_dataframe(result, table_id, job_config)
        job.result()
        df = get_unprocessed_articles(bq_client)
        batch_run += 1
        total_rows += len(result)
        log.info(f'Batch {batch_run} - Wrote {len(result)} sentiment articles to {TARGET_TABLE}')
    else:
        log.info(f'All articles have been processed. {total_rows} rows wrote this run.')