WITH traders AS (
    SELECT * FROM {{ref ('traders')}}
)

SELECT 
    CAST(trader_id AS VARCHAR) AS trader_id,
    CAST(name AS VARCHAR) AS name,
    CAST(days_active AS INTEGER) AS days_active,
    CAST(subscribers AS INTEGER) AS subscribers,
    CAST(win_rate AS DOUBLE) AS win_rate,
    CAST(max_drawdown AS DOUBLE) AS max_drawdown,
    CAST(lifetime_pnl AS DOUBLE) AS lifetime_pnl,
    CAST(um_margin_balance AS DOUBLE) AS um_margin_balance,
    CAST(cm_margin_balance AS DOUBLE) AS cm_margin_balance,
    CAST(net_transfer AS DOUBLE) AS net_transfer,
    CAST(profile_roi AS DOUBLE) AS profile_roi
FROM traders