--ref from stg__stock_prices, add cols company names, sector from assets
-- add more metrics: moving average 7 & 30 days, voltality 7 days
WITH prices AS (
    SELECT * 
    FROM {{ ref('stg__stock_prices') }}
),

names AS (
    SELECT
        symbol,
        company_name,
        sector
    FROM {{ ref('dim__assets') }}
)

SELECT
    p.symbol,
    n.company_name,
    n.sector,
    p.trade_date,
    p.open,
    p.high,
    p.low,
    p.close,
    p.volume,
    p.vwap,
    p.trade_count,
    p.daily_return_pct,

    -- adding metrics:
    ROUND(AVG(p.close) OVER(PARTITION BY p.symbol ORDER BY p.trade_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),4) AS moving_avg_7d,
    ROUND(AVG(p.close) OVER(PARTITION BY p.symbol ORDER BY p.trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),4) AS moving_avg_30d,
    ROUND(STDDEV(p.daily_return_pct) OVER(PARTITION BY p.symbol ORDER BY p.trade_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),4) AS rolling_vol_7d

FROM prices p 
LEFT JOIN names n 
    ON p.symbol = n.symbol