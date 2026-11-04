WITH p AS (
    SELECT * FROM {{ref('positions')}}
)

SELECT
    position_id,
    trader_id,
    symbol,
    side,
    status,
    margin_mode,
    leverage,
    entry_price,
    close_price,
    opened_volume,
    closed_volume,
    remaining_volume,
    max_open_interest,
    gross_pnl,
    funding_pnl,
    trading_fee,
    liquidation_fee,
    net_pnl,
    roi,
    pnl_currency,
    opened_at,
    closed_at,
    updated_at,
FROM p