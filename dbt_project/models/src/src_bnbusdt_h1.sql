WITH raw_bnb_h1 AS (
    SELECT * FROM {{source('raw_crypto_data', 'bnbusdt_h1')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_bnb_h1