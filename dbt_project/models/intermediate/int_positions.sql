{#
    dbt_project/models/intermediate/int_positions.sql
#}


-- Casts happen here rather than in the body so the surrogate key and the output
-- column hash the same value. Hashing the raw epoch milliseconds while the
-- output column is a timestamp would make the key impossible to recompute from
-- this table. Casting first also removes the ambiguity from ORDER BY opened_at.
WITH _pos AS (
    SELECT
        * EXCLUDE (opened_at, closed_at, updated_at),
        to_timestamp(opened_at / 1000.0) AS opened_at,
        to_timestamp(closed_at / 1000.0) AS closed_at,
        to_timestamp(updated_at / 1000.0) AS updated_at
    FROM {{ ref('stg_positions') }}
)

SELECT 
    {{dbt_utils.generate_surrogate_key(['position_id', 'trader_id', 'opened_at', 'side'])}} AS position_sk,
    position_id,
    trader_id,
    symbol,
    side,
    status,
    leverage,
    entry_price,
    close_price,
    opened_volume,
    closed_volume,
    remaining_volume,
    gross_pnl,
    funding_pnl,
    trading_fee,
    liquidation_fee,
    net_pnl,
    roi,
    opened_at,
    closed_at,
    updated_at
FROM _pos
WHERE symbol IN (SELECT symbol FROM {{ref('stg_symbols')}})
ORDER BY opened_at ASC
