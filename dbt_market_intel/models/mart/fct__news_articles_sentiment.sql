-- join news_articles and news_sentiment, choose inner join to take articles with sentiment only
WITH news AS (
    SELECT * 
    FROM {{ ref('stg__news_articles') }}
),

sentiment AS (
    SELECT *
    FROM {{ ref('stg__news_sentiment') }}
)

SELECT
    n.article_id,
    n.symbol,
    n.title,
    n.description_clean,
    n.source_name,
    n.published_date,
    n.url,
    s.sentiment,
    s.sentiment_score,
    s.confidence
FROM news n
INNER JOIN sentiment s
ON n.article_id = s.article_id