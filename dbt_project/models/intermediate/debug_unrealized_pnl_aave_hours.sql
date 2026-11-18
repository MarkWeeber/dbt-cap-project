{#
dbt_project/models/intermediate/debug_unrealized_pnl_aave_hours.sql

Debug artifact, not part of the report layer. Expands the mark-to-market macro
hour by hour for one position so the size switch can be eyeballed.
#}

{{ config(enabled=var('debug', false)) }}

WITH pos AS (
    SELECT
        position_id,
        trader_id,
        side,
        entry_price,
        opened_volume,
        remaining_volume,
        status,
        updated_at,
        opened_at,
        closed_at
    FROM {{ ref('int_positions') }}
    WHERE symbol = 'AAVEUSDT'
),

priced AS (
    SELECT "timestamp" AS hour, "close" AS mark
    FROM {{ ref('src_aaveusdt_h1') }}
)

SELECT
    h.hour,
    p.position_id,
    p.status,
    p.opened_volume,
    p.remaining_volume,
    p.trader_id,

    -- copy of the macro's size selection, only so the chosen size is visible
    CASE
        WHEN p.status = 'CLOSED' THEN p.opened_volume
        WHEN p.status = 'PARTIALLY_CLOSED'
             AND p.updated_at IS NOT NULL
             AND date_trunc('hour', p.updated_at) <= h.hour
        THEN p.remaining_volume
        ELSE p.opened_volume
    END AS volume_used,

    p.entry_price,
    h.mark,
    {{ total_unrealized_pnl_hours(
        'p.side', 'p.entry_price', 'p.opened_volume', 'p.remaining_volume',
        'p.status', 'p.updated_at', 'h.hour', 'h.mark') }} AS unrealized_pnl
FROM priced h
LEFT JOIN pos p
    ON {{ positions_is_within_hour('h.hour', 'p.opened_at', 'p.closed_at') }}
WHERE h.hour BETWEEN '2026-06-17 00:00:00+00' AND '2026-06-30 00:00:00+00'
ORDER BY h.hour
