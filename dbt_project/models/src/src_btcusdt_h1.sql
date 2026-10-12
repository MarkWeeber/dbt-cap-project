WITH raw_btc_h1 AS (
    SELECT * FROM {{source('raw_crypto_data', 'btcusdt_h1')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_btc_h1