{#
dbt_project/models/intermediate/int_unrealized_pnl_btc_hourly.sql

One row per hour of the analysis window, one column per trader, holding that
trader's aggregate unrealized PnL on BTCUSDT across every position open during
the hour.

Mark price is the close of the hour, so a position opened part-way through an
hour is valued after its entry rather than before it.

Sign convention follows the broker: positive means the position is in profit.

Only BTCUSDT is covered here. Traders are generated as columns from stg_traders
at compile time, so a renamed trader changes the schema and needs a
--full-refresh.
#}

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
        closed_at,
        gross_pnl,
        opened_at
    FROM {{ ref('int_positions') }}
    WHERE symbol = 'BTCUSDT'
),

btc_price AS (
    SELECT
        "timestamp",
        "close" AS symbol_close_price
    FROM {{ ref('src_btcusdt_h1') }}
)

SELECT
    t.hour AS time,

    {%- for trader in traders %}

    SUM(
        CASE WHEN p.trader_id = {{ trader['trader_id'] }}
             THEN {{ total_unrealized_pnl_hours('p.side', 'p.entry_price', 'p.opened_volume', 'b.symbol_close_price', 'p.closed_at', 't.hour', 'p.gross_pnl') }}
             ELSE 0 END
    ) AS "{{ col_names[loop.index0] }}"{% if not loop.last %},{% endif %}

    {%- endfor %}

FROM {{ ref('int_h1_timeline') }} t

-- narrows the union grid to BTCUSDT on purpose: this model needs a BTC mark price
INNER JOIN btc_price b
    ON b."timestamp" = t.hour

LEFT JOIN filtered_positions p
    ON {{ positions_is_within_hour('t.hour', 'p.opened_at', 'p.closed_at') }}

GROUP BY t.hour, b.symbol_close_price
ORDER BY t.hour