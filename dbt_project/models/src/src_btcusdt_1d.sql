WITH raw_btc_1d AS (
    SELECT * FROM {{source('raw_crypto_data', 'btcusdt_1d')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_btc_1d