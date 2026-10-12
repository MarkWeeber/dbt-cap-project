WITH raw_btc_d1 AS (
    SELECT * FROM {{source('raw_crypto_data', 'btcusdt_d1')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_btc_d1