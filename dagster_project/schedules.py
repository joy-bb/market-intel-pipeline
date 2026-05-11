from dagster import AssetSelection, define_asset_job, ScheduleDefinition

# job 1: Ingestion only, run during market hours
ingestion_job = define_asset_job(
    name = "ingestion_job",
    selection = AssetSelection.assets(
        "raw_stock_prices",
        "raw_news_articles",
        "raw_news_sentiment"
    )
)

# Job 2: dbt transformation only run after market close
transform_job = define_asset_job(
    name = "transform_job",
    selection = AssetSelection.all() - AssetSelection.assets(
        "raw_stock_prices",
        "raw_news_articles", 
        "raw_news_sentiment"
    )
)

# Job 3: backfill job, load historical or missing data
# Only appear on UI but run on DEMANDS ONLY

backfill_job = define_asset_job(
    name = "backfill_job",
    selection = AssetSelection.assets(
        "raw_stock_prices",
        "raw_news_articles",
        "raw_news_sentiment"
    ),
    description = "Manual backfill job - trigger from UI when needed"
)

# Schedule 1: everyday from Tuesaday to Saturday, at 7am
ingestion_schedule = ScheduleDefinition(
    name = "daily_ingestion_schedule",
    cron_schedule = "0 7 * * 2-6",
    job = ingestion_job,
    execution_timezone = "Europe/London"
)
# Schedule 2: everyday from Tuesday to Saturday, at 8:30
transform_schedule = ScheduleDefinition(
    name = "daily_transformation_schedule",
    cron_schedule = "30 8 * * 2-6",
    job = transform_job,
    execution_timezone = "Europe/London"
)
