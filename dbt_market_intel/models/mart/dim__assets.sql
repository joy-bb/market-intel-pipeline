SELECT
    symbol,
    company_name,
    sector
FROM {{ ref('companies_info') }}