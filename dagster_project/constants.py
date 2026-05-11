import os
from dotenv import load_dotenv

load_dotenv()

# GCP
PROJECT_ID = os.environ["GCP_PROJECT_ID"]

# Alpaca
ALPACA_API_KEY = os.environ["ALPACA_API_KEY"]
ALPACA_SECRET_KEY = os.environ["ALPACA_SECRET_KEY"]

# NewsAPI
NEWSAPI_KEY = os.environ["NEWS_API_KEY"]

# Groq
GROQ_API_KEY = os.environ["GROQ_API_KEY"]

TICKERS = ["AAPL", "MSFT", "GOOGL", "META", "NVDA", "AMZN", "TSLA", "JPM", "V", "JNJ"]

DATASET = "raw"
BACKFILL_DAYS = 29
BATCH_SIZE = 50
MODEL = "llama-3.1-8b-instant"
TARGET_TABLE = "news_sentiment"