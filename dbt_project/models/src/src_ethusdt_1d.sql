WITH raw_eth_1d AS (
    SELECT * FROM {{source('raw_crypto_data', 'ethusdt_1d')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_eth_1d