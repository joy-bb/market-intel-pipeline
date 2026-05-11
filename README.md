# Financial Market Intelligence Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue)
![dbt](https://img.shields.io/badge/dbt-1.8-orange)
![Dagster](https://img.shields.io/badge/Dagster-1.8-purple)
![Streamlit](https://img.shields.io/badge/Streamlit-1.39-red)
![BigQuery](https://img.shields.io/badge/BigQuery-Google_Cloud-blue)

## Project overview

This project is an end-to-end data engineering pipeline that collects daily stock market data from the Alpaca API and financial news from the NewsAPI, then enriches the news using AI-powered sentiment analysis through Groq. The pipeline automatically ingests, transforms, and stores the data in Google BigQuery using Dagster and dbt, before serving it through an interactive Streamlit dashboard. The dashboard visualizes stock performance metrics such as moving averages, daily returns, and AI-generated news sentiment summaries alongside market activity. Its main goal is to help users explore the relationship between public news sentiment and stock price movements, including whether positive or negative news appears to influence price changes before or after publication.

## Architecture
```
Alpaca Markets API          NewsAPI
      │                        │
      ▼                        ▼
Python ingestion         Python ingestion
(incremental,            (dedup MD5,
 watermark)               defensive)
      │                        │
      ▼                        ▼
BigQuery                 BigQuery
raw.stock_prices         raw.news_articles
      │                        │
      │                        ▼
      │                 Groq LLM Enrichment
      │                 (reads raw.news_articles)
      │                 (writes raw.news_sentiment)
      │                        │
      │                        ▼
      │                 BigQuery
      │                 raw.news_sentiment
      │                        │
      ▼                        ▼
      └──────────────────────► dbt Core transforms
                               Silver: stg_* models
                               Gold:   fct_* + mart_*
                                        │
                                        ▼
                               BigQuery
                               silver.* + gold.*
                                        │
                                        ▼
                               Streamlit Dashboard
                               (price + sentiment)

Dagster orchestrates all steps
Schedule: Tuesday–Saturday, 7AM + 8:30AM GMT+1
```

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Stock prices | Alpaca API | Pull daily stock market price data |
| News | NewsAPI | Collect financial news articles |
| LLM enrichment | Groq (llama-3.1-8b-instant) | Sentiment classification on news articles |
| Storage | BigQuery | Store raw and transformed datasets|
| Transform | dbt Core | Transform and model analytics-ready tables |
| Orchestration | Dagster | Schedule and orchestrate pipeline jobs|
| Dashboard | Streamlit | Build interactive analytics dashboard |

## Project Structure

```
market-intel-pipeline/
├── ingestion/
│   ├── ingest_stock_prices.py    # Alpaca API ingestion
│   ├── ingest_news_articles.py   # NewsAPI ingestion  
│   └── enrich_news_sentiment.py  # Groq LLM enrichment
├── dbt_market_intel/
│   ├── models/
│   │   ├── staging/                        # Silver: clean, validate, deduplicate
│   │   │   ├── stg__stock_prices.sql
│   │   │   ├── stg__news_articles.sql
│   │   │   ├── stg__news_sentiment.sql
│   │   │   └── _staging_tests.yml
│   │   └── marts/                          # Gold: aggregate, join, serve
│   │       ├── fct__daily_prices.sql
│   │       ├── fct__news_sentiment.sql
│   │       ├── mart_prices_news_sentiment.sql
│   │       ├── dim__assets.sql
│   │       └── _mart_tests.yml
│   └── seeds/
│       └── companies_info.csv
├── dagster_project/
│   ├── __init__.py                         # Definitions() — wires everything together
│   ├── constants.py                        # Shared config — API keys, project ID, tickers
│   ├── schedules.py                        # 3 jobs + 2 schedules (ingestion + transform)
│   ├── sensors.py                          # Failure sensor — n8n → Slack (scaffolded, see Future Improvements)
│   └── assets/
│       ├── __init__.py                     # Makes assets/ a Python package
│       ├── ingestion.py                    # @asset: raw_stock_prices, raw_news_articles
│       ├── enrichment.py                   # @asset: raw_news_sentiment (Groq LLM)
│       └── dbt_assets.py                   # @dbt_assets: all 7 dbt models auto-registered
├── app/
│   └── dashboard.py                        # Streamlit dashboard
└── README.md
```

## Running Locally

### Prerequisites
- Python 3.11+
- GCP account with BigQuery enabled
- GCP service account JSON key with BigQuery Data Editor + Job User roles
- API keys: Alpaca, NewsAPI, Groq

### Setup

1. Clone the repo
```bash
git clone https://github.com/joy-bb/market-intel-pipeline
cd market-intel-pipeline
```

2. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Configure environment variables
```bash
cp .env.example .env
# Fill in your API keys in .env
```

5. Run ingestion
```bash
python ingestion/ingest_stock_prices.py
python ingestion/ingest_news_articles.py
python ingestion/enrich_news_sentiment.py
```

6. Run dbt transforms
```bash
cd dbt_market_intel
dbt deps
dbt seed
dbt run
dbt test
```

7. Start Dagster
```bash
dagster dev
```

8. Start dashboard
```bash
streamlit run app/dashboard.py
```

## Engineering Notes

### API Design Differences & Defensive Coding

One of the most interesting parts of this project was handling two APIs with very different designs. Although both provide data for the same pipeline, the ingestion and transformation logic needed to be implemented differently depending on the structure and reliability of each source.

### Pull Data Function Comparison

| Aspect | Stock API | News API | Why Different? |
|---|---|---|---|
| Number of API calls | 1 call for all tickers | 1 call per ticker | Alpaca accepts multiple symbols in one request, while NewsAPI only supports one query at a time |
| Loop structure | Single loop over response | Nested loops over tickers and articles | Response structures are different |
| Collecting results | Structured object returned directly | `.extend()` used to flatten article lists | NewsAPI responses must be manually combined |
| Adding symbol | Included automatically | Manually stamped onto each article | NewsAPI does not retain the original ticker context |
| Deduplication | Not needed | `drop_duplicates()` on article IDs | Same article can appear in multiple searches |

### DataFrame Conversion Comparison

| Aspect | Stock Data | News Data | Why Different? |
|---|---|---|---|
| Input type | Custom `BarSet` object | List of dictionaries | Alpaca SDK returns typed objects while NewsAPI returns raw JSON |
| Loop structure | Nested loops | Single loop | Stock data is grouped by symbol |
| Field access | Dot notation (`bar.open`) | Dictionary access (`article.get()`) | Different object types |
| Defensive checks | Minimal | URL existence checks | News data can contain missing or malformed fields |
| Unique ID handling | Natural key exists (`symbol + date`) | MD5 hash generated from URL | Articles do not have reliable unique IDs |
| Date handling | Native datetime object | Manual string parsing | NewsAPI timestamps arrive as raw strings |
| Deduplication | Not required | Required | Articles may overlap across ticker searches |

**Key Engineering Takeaway**  
Clean and strongly structured APIs allow simpler ingestion logic, while real-world APIs often require defensive coding to handle missing, duplicated, or inconsistent data. Building both ingestion pipelines helped me better understand how upstream API design directly impacts downstream transformation, data quality, and analytics reliability.

## Future Improvements

1. **Streaming data** — The current pipeline uses daily market data from the Alpaca free tier. In the future, I would like to ingest streaming or intraday data to capture price movements in smaller timeframes and make the dashboard more responsive to real-time market events and news sentiment changes.

2. **Lakehouse architecture** — For simplicity, this project stores both raw and transformed data in BigQuery. As the project scales, I would redesign the architecture to store raw immutable data in S3 or object storage while keeping BigQuery as the serving warehouse layer. This would better reflect a production-style lakehouse architecture and reduce long-term storage costs at larger data volumes.

3. **Intermediate dbt layer** — Since the project currently involves only two fact tables and relatively simple joins, the transformation logic fits cleanly within the mart models. In a larger pipeline with more complex multi-source transformations, I would introduce an intermediate dbt layer to separate staging, business logic, and serving models more clearly and improve maintainability.

4. **Failure alerting** — `sensors.py` is scaffolded and ready for a Dagster `RunFailureSensor` that posts to an n8n webhook, which forwards a Slack notification on any failed pipeline run. Decoupling the alert layer from Dagster means notification channels (Slack, email, PagerDuty) can be changed without touching pipeline code.

## Dashboard

![Dashboard screenshot](screenshot1.png)
![Dashboard screenshot](screenshot2.png)