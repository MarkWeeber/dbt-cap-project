WITH raw_btc_1h AS (
    SELECT * FROM {{source('raw_crypto_data', 'btcusdt_1h')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_btc_1h