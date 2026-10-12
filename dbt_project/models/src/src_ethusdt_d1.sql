WITH raw_eth_d1 AS (
    SELECT * FROM {{source('raw_crypto_data', 'ethusdt_d1')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_eth_d1