WITH raw_doge_h1 AS (
    SELECT * FROM {{source('raw_crypto_data', 'dogeusdt_h1')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_doge_h1