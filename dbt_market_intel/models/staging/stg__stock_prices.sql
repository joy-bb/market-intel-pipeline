-- silver layer: clean stock data and add daily-return 
WITH source AS (
    SELECT *
    FROM {{ source('raw', 'stock_prices') }}
),

edit AS (
    SELECT
        symbol,
        bar_date AS trade_date,
        open,
        high,
        low,
        close,
        volume,
        vwap,
        trade_count,
        ROUND((close - LAG(close) OVER(PARTITION BY symbol ORDER BY bar_date))
        / NULLIF(LAG(close) OVER(PARTITION BY symbol ORDER BY bar_date),0)
        *100,4) AS daily_return_pct
    FROM source
)

SELECT * FROM edit