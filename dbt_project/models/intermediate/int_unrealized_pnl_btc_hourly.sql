{#
dbt_project/models/intermediate/int_unrealized_pnl_btc_hourly.sql

One row per hour of the analysis window, one column per trader, holding that
trader's aggregate unrealized PnL on the filtered symbol across every position
open during the hour.

Mark price is the close of the hour, so a position opened part-way through an
hour is valued after its entry rather than before it.

Sign convention follows the broker: positive means the position is in profit.

A partially closed position is marked on its remaining size from the hour of the
partial close onwards; the realized leg stays in net_pnl and does not appear
here, so the column drops sharply at that hour by design.

Traders are generated as columns from stg_traders at compile time, so a renamed
trader changes the schema and needs a --full-refresh.
#}

{%- set filter_symbol = 'BTCUSDT' -%}
{%- set market_values_source = 'stg_btcusdt_h1' -%}

{%- set get_traders_query -%}
    SELECT trader_id, name
    FROM {{ ref('stg_traders') }}
    WHERE trader_id IS NOT NULL
    ORDER BY trader_id
{%- endset -%}

{%- set results = run_query(get_traders_query) -%}

{%- if execute -%}
    {%- set traders = results.rows -%}
{%- else -%}
    {%- set traders = [] -%}
{%- endif -%}

{%- set col_names = trader_column_names(traders, '_btc_pnl') -%}

WITH filtered_positions AS (
    SELECT
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
    WHERE symbol = '{{ filter_symbol }}'
),

symbol_price AS (
    SELECT
        "timestamp",
        "close" AS symbol_close_price
    FROM {{ ref(market_values_source) }}
)

SELECT
    t.hour AS time,
    {%- for trader in traders %}

    SUM(
        CASE WHEN p.trader_id = {{ trader['trader_id'] }}
             THEN {{ total_unrealized_pnl_hours(
                        'p.side',
                        'p.entry_price',
                        'p.opened_volume',
                        'p.remaining_volume',
                        'p.status',
                        'p.updated_at',
                        't.hour',
                        'b.symbol_close_price') }}
             ELSE 0 END
    ) AS "{{ col_names[loop.index0] }}"{% if not loop.last %},{% endif %}

    {%- endfor %}

FROM {{ ref('int_h1_timeline') }} t

-- narrows the union grid to the filtered symbol on purpose: this model needs a
-- price series for it
INNER JOIN symbol_price b
    ON b."timestamp" = t.hour

LEFT JOIN filtered_positions p
    ON {{ positions_is_within_hour('t.hour', 'p.opened_at', 'p.closed_at') }}

GROUP BY t.hour
ORDER BY t.hour