from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from dagster_project.assets import ingestion, enrichment
from dagster_project.assets.dbt_assets import market_intel_dbt_assets, DBT_PROJECT_DIR
from dagster_project.schedules import(
    ingestion_job,
    transform_job,
    backfill_job,
    ingestion_schedule,
    transform_schedule
)

python_assets = load_assets_from_modules([ingestion,enrichment])

defs = Definitions(
    assets = [
        *python_assets,
        market_intel_dbt_assets
    ],
    resources = {
        "dbt": DbtCliResource(
            project_dir = str(DBT_PROJECT_DIR)
        )
    },
    schedules = [ingestion_schedule,transform_schedule],
    jobs = [ingestion_job, transform_job, backfill_job]
)