-- remove duplicate article_id, validate sentiment, clamp confidence_score
WITH source AS (
    SELECT * 
    FROM {{ source('raw','news_sentiment') }}
),

deduplicate AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER(PARTITION BY article_id ORDER BY _enriched_at DESC) = 1
)

SELECT
    article_id,
    CASE
        WHEN LOWER(sentiment) IN ('positive','negative','neutral')
            THEN LOWER(sentiment)
        ELSE 'neutral'
        END AS sentiment,
    sentiment_score,
    GREATEST(0.0, LEAST(1.0,confidence)) as confidence
FROM deduplicate