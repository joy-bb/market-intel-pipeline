import os 
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery

from groq import Groq
import json

load_dotenv()

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GROQ_API_KEY = os.environ["GROQ_API_KEY"]
DATASET = "raw"
SOURCE_TABLE = "news_articles"
TARGET_TABLE = "news_sentiment"
BATCH_SIZE = 50
MODEL = "llama-3.1-8b-instant"

SYSTEM_PROMPT = """ You're a expertise financial news sentiment classifier.
You will recieve news article headlines and description.
Classify the sensiment from the perspective of stock investor:
-'positive': good news for company or stock
-'negative': bad news for company or stock
-'neutral': no clear directional signal
Response ONLY with JSON in this exact format, no explaination, no markdown:
{"sentiment":"positive","confidence":0.95} 
confidence is your certainty from 0.0 (very unsure) to 1.0 (certain).
"""
def classify_sentiment(groq_client, title, description):
    try:
        response = groq_client.chat.completions.create(
            model = MODEL,
            messages = [
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user','content': f'Headline:{title} \n\nDescription: {description}'}
            ],
            max_tokens = 60,
            temperature = 0.1
        )
        raw_text = response.choices[0].message.content.strip()
        raw_text = raw_text.replace('```json','').replace('```','').strip()
        result = json.loads(raw_text)

        sentiment = result.get('sentiment','neutral').lower()
        if sentiment not in ('positive','negative','neutral'):
            sentiment = 'neutral'
        confidence = float(result.get('confidence',0.5))
        confidence = max(0.0, min(1.0, confidence))

        return {'sentiment': sentiment, 'confidence': confidence}

    except Exception:
        return {'sentiment': 'neutral','confidence': 0.0}
    
# Getting unprocess articles from bigquery
def get_unprocessed_articles(bq_client):
    check_query = f"""
        SELECT article_id
        FROM `{PROJECT_ID}.{DATASET}.{TARGET_TABLE}`
    """
    query1 = f"""
        SELECT article_id, title, description
        FROM `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}`
        LIMIT {BATCH_SIZE}
    """
    query2 = f"""
        SELECT s.article_id, s.title, s.description 
        FROM `{PROJECT_ID}.{DATASET}.{SOURCE_TABLE}` s
        LEFT JOIN `{PROJECT_ID}.{DATASET}.{TARGET_TABLE}` t
        ON s.article_id = t.article_id
        WHERE t.article_id is NULL
        LIMIT {BATCH_SIZE}
    """
    try:
        bq_client.query(check_query).result()
        query = query2
    except Exception:
        print(f'First run - sentiment table will be created and all articles will be processed')
        query = query1

    try:
        df = bq_client.query(query).to_dataframe()
        return df
    except Exception:
        return pd.DataFrame()

# main
def main():
    bq_client = bigquery.Client(project = PROJECT_ID)
    df = get_unprocessed_articles(bq_client)
    
    groq_client = Groq(api_key = os.environ['GROQ_API_KEY'])
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
        print(f'Batch {batch_run} - Wrote {len(result)} sentiment articles to {TARGET_TABLE}')
    else:
        print(f'All articles have been processed. {total_rows} rows wrote this run.')

if __name__ == '__main__':
    main()
            