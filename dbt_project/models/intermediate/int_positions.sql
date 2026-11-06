WITH _pos AS (
    SELECT * FROM {{ref('stg_positions')}}
)

SELECT 
    {{dbt_utils.generate_surrogate_key(['position_id', 'trader_id', 'opened_at', 'side'])}} AS position_id,
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
    to_timestamp(opened_at / 1000.0) AS opened_at,
    to_timestamp(closed_at / 1000.0) AS closed_at,
FROM _pos
WHERE symbol IN (SELECT symbol FROM {{ref('stg_symbols')}})
ORDER BY opened_at ASC