WITH raw_eth_1h AS (
    SELECT * FROM {{source('raw_crypto_data', 'ethusdt_1h')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_eth_1h