WITH raw_sol_h1 AS (
    SELECT * FROM {{source('raw_crypto_data', 'solusdt_h1')}}
)

SELECT 
    timestamp,
    open,
    high,
    low,
    close,
    volume
FROM raw_sol_h1