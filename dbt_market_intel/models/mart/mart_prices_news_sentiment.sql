WITH price AS (
    SELECT * 
    FROM {{ ref('fct__daily_prices_enriched') }}
),

daily_sentiment AS (
    SELECT
        symbol,
        published_date,
        AVG(sentiment_score) AS avg_sentiment_score,
        COUNT(*) AS article_count,
        COUNTIF(sentiment = 'positive') AS positive_count,
        COUNTIF(sentiment = 'negative') AS negative_count,
        COUNTIF(sentiment = 'neutral') AS neutral_count
    FROM {{ ref('fct__news_articles_sentiment') }}
    GROUP BY symbol, published_date
)

SELECT
    p.symbol,
    p.company_name,
    p.sector,
    p.trade_date,
    p.open,
    p.high,
    p.low,
    p.close,
    p.volume,
    p.vwap,
    p.trade_count,
    p.daily_return_pct,
    p.moving_avg_7d,
    p.moving_avg_30d,
    p.rolling_vol_7d,

    COALESCE(d.avg_sentiment_score,0) AS avg_sentiment_score,
    COALESCE(d.article_count,0) AS article_count,
    COALESCE(d.positive_count,0) AS positive_count,
    COALESCE(d.negative_count,0) AS negative_count,
    COALESCE(d.neutral_count,0) AS neutral_count,

    -- rolling sentiment_count 3d
    ROUND(AVG(d.avg_sentiment_score) OVER(PARTITION BY p.symbol ORDER BY p.trade_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),2) AS rolling_sentiment_3d
FROM price p
LEFT JOIN daily_sentiment d
    ON p.symbol = d.symbol
    AND p.trade_date = d.published_date