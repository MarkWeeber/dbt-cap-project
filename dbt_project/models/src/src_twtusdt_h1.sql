WITH raw_twt_h1 AS (
    SELECT * FROM {{source('raw_crypto_data', 'twtusdt_h1')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_twt_h1