WITH symbols AS (
    SELECT * FROM {{ref('symbols')}}
)

SELECT
    symbol,
    base_asset,
    quote_asset
FROM symbols