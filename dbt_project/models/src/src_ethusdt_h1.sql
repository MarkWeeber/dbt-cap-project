WITH raw_eth_h1 AS (
    SELECT * FROM {{source('raw_crypto_data', 'ethusdt_h1')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_eth_h1