WITH raw_aave_h1 AS (
    SELECT * FROM {{source('raw_crypto_data', 'aaveusdt_h1')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_aave_h1