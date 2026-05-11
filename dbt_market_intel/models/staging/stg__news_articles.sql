--silver layer: remove duplicate articles_id, clean description
WITH source AS (
    SELECT * 
    FROM {{ source('raw','news_articles') }}
),

deduplicate AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY _loaded_at DESC) = 1
)

SELECT
    article_id,
    symbol,
    title,
    TRIM(description) AS description_clean,
    source_name,
    published_date,
    url
FROM deduplicate
