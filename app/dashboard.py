import os
from dotenv import load_dotenv

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from google.cloud import bigquery

load_dotenv()

PROJECT_ID = os.environ["GCP_PROJECT_ID"]

st.set_page_config(
    page_title = "Market Intelligence Dashboard",
    layout = "wide"
)

st.title("Financial Market Intelligence Pipeline")
st.caption("Daily stock prices + LLM sentiment analysis · Alpaca + NewsAPI + Groq")

# Create cache resource for bigqueryclient, 
# 2 cases: running on Streamlit cloud, and run locally
@st.cache_resource
def get_bq_client():
    # Check if running on Streamlit Cloud
    if "gcp_service_account" in st.secrets:
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(
            project=st.secrets["gcp_service_account"]["project_id"],
            credentials=credentials
        )
    else:
        # Running locally — use gcp-key.json
        return bigquery.Client(project=os.environ["GCP_PROJECT_ID"])

# Create cache data for 1hr 
@st.cache_data(ttl=3600)
def load_price_sensitment(symbol):
    client = get_bq_client()
    query = f"""
        SELECT 
            trade_date,
            close,
            moving_avg_7d,
            moving_avg_30d,
            daily_return_pct,
            rolling_vol_7d,
            avg_sentiment_score,
            article_count,
            positive_count,
            negative_count,
            neutral_count,
            rolling_sentiment_3d
        FROM `{PROJECT_ID}.gold.mart_prices_news_sentiment`
        WHERE symbol = @symbol
        ORDER BY trade_date DESC
        LIMIT 180
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters = [
            bigquery.ScalarQueryParameter("symbol","STRING",symbol)
        ]
    )
    df = client.query(query,job_config=job_config).to_dataframe()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df.sort_values("trade_date")

@st.cache_data(ttl=3600)
def load_news_feed(symbol, limit=10):
    client = get_bq_client()
    query = f"""
        SELECT
            title,
            source_name,
            published_date,
            sentiment,
            confidence,
            url
        FROM `{PROJECT_ID}.gold.fct__news_articles_sentiment`
        WHERE symbol = @symbol
        ORDER BY published_date DESC
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters = [
            bigquery.ScalarQueryParameter("symbol","STRING",symbol),
            bigquery.ScalarQueryParameter("limit","INT64", limit)
        ]
    )
    return client.query(query, job_config= job_config).to_dataframe()

# Side bar
TICKERS = ["AAPL", "MSFT", "GOOGL", "META", "NVDA", "AMZN", "TSLA", "JPM", "V", "JNJ"]

with st.sidebar:
    st.header("Controls")
    symbol = st.selectbox(
        "Select ticker",
        options = TICKERS
    )
    lookback = st.slider(
        "Days to display",
        min_value = 30,
        max_value = 180,
        value = 90,
        step = 30
    )

#load data
with st.spinner(f"Loading data for {symbol}..."):
    try:
        df = load_price_sensitment(symbol)
        news_df = load_news_feed(symbol)
        data_loaded = True
    except Exception as e:
        st.error(f"Could not connect to BigQuery: {e}")
        data_loaded = False

if not data_loaded:
    st.stop()

df = df.tail(lookback)

# Metrics row 
if not df.empty:
    latest = df.iloc[-1]

    col1,col2,col3,col4,col5 = st.columns(5)
    col1.metric("Close", f"${latest['close']:.2f}", f"{latest['daily_return_pct']:+.2f}%")
    col2.metric("7-Day MA", f"${latest['moving_avg_7d']:.2f}")
    col3.metric("30-Day MA",f"${latest['moving_avg_30d']:.2f}")
    col4.metric("Sentiment", f"{latest['avg_sentiment_score']:+.2f}", f"{latest['article_count']:.0f} articles")
    col5.metric("Volatility 7d", f"{latest['rolling_vol_7d']:.2f}%")

# Main chart
st.subheader(f"{symbol} — Price & Sentiment · Last {lookback} days")

fig = make_subplots(
    rows=3, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.04,
    row_heights=[0.55, 0.25, 0.20],
    subplot_titles=["Price + Moving Averages", "Daily Sentiment", "Sentiment Momentum (3d)"]
)

# Row 1: Price lines
fig.add_trace(go.Scatter(
    x=df["trade_date"], y=df["close"],
    name="Close", line=dict(color="#4A9EFF", width=1.5)
), row=1, col=1)

fig.add_trace(go.Scatter(
    x=df["trade_date"], y=df["moving_avg_7d"],
    name="7d MA", line=dict(color="#FFB347", width=1, dash="dot")
), row=1, col=1)

fig.add_trace(go.Scatter(
    x=df["trade_date"], y=df["moving_avg_30d"],
    name="30d MA", line=dict(color="#FF6B6B", width=1, dash="dot")
), row=1, col=1)

# Row 2: Sentiment bars
colors = df["avg_sentiment_score"].apply(
    lambda x: "#4CAF50" if x > 0 else ("#EF5350" if x < 0 else "#9E9E9E")
).tolist()

fig.add_trace(go.Bar(
    x=df["trade_date"], y=df["avg_sentiment_score"],
    name="Sentiment", marker_color=colors, showlegend=False
), row=2, col=1)

# Row 3: Sentiment momentum
fig.add_trace(go.Scatter(
    x=df["trade_date"], y=df["rolling_sentiment_3d"],
    name="Momentum 3d", line=dict(color="#AB47BC", width=2),
    fill="tozeroy", fillcolor="rgba(171, 71, 188, 0.1)"
), row=3, col=1)

fig.add_hline(y=0, line_dash="solid", line_color="rgba(128,128,128,0.3)", row=2, col=1)
fig.add_hline(y=0, line_dash="solid", line_color="rgba(128,128,128,0.3)", row=3, col=1)

fig.update_layout(
    height=600,
    margin=dict(l=0, r=0, t=30, b=0),
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)

st.plotly_chart(fig, use_container_width=True)

# Sentiment breakdown
st.subheader("Sentiment breakdown by day")

fig2 = go.Figure()
fig2.add_trace(go.Bar(
    x=df["trade_date"], y=df["positive_count"],
    name="Positive", marker_color="#4CAF50"
))
fig2.add_trace(go.Bar(
    x=df["trade_date"], y=df["neutral_count"],
    name="Neutral", marker_color="#9E9E9E"
))
fig2.add_trace(go.Bar(
    x=df["trade_date"], y=df["negative_count"],
    name="Negative", marker_color="#EF5350"
))
fig2.update_layout(
    barmode="stack",
    height=200,
    margin=dict(l=0, r=0, t=10, b=0),
    legend=dict(orientation="h"),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)
st.plotly_chart(fig2, use_container_width=True)

# News feed
st.subheader(f"Latest news — {symbol}")

BADGE = {"positive": "🟢", "negative": "🔴", "neutral": "⚪"}

if news_df.empty:
    st.info("No classified articles found.")
else:
    for _, row in news_df.iterrows():
        badge = BADGE.get(row["sentiment"], "⚪")
        confidence_pct = int(row["confidence"] * 100)
        st.markdown(
            f"{badge} **[{row['title']}]({row['url']})**  \n"
            f"*{row['source_name']} · {row['published_date']} · "
            f"{row['sentiment'].capitalize()} ({confidence_pct}% confidence)*"
        )
        st.divider()