WITH src_btcusdt_d1 AS (
    SELECT * FROM {{ref ('src_btcusdt_d1')}}
)

SELECT 
    CAST(timestamp AS TIMESTAMP WITH TIME ZONE) AS timestamp,
    CAST(open AS DOUBLE) AS open,
    CAST(high AS DOUBLE) AS high,
    CAST(low AS DOUBLE) AS low,
    CAST(close AS DOUBLE) AS close,
    CAST(volume AS DOUBLE) AS volume
FROM src_btcusdt_d1