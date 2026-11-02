WITH raw_ltc_h1 AS (
    SELECT * FROM {{source('raw_crypto_data', 'ltcusdt_h1')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_ltc_h1