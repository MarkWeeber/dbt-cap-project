WITH raw_xrp_h1 AS (
    SELECT * FROM {{source('raw_crypto_data', 'xrpusdt_h1')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_xrp_h1