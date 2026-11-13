{#
dbt_project/models/intermediate/int_open_position_counts_btc_hourly.sql

One row per hour of the analysis window, one column per trader, holding the
number of that trader's BTCUSDT positions open at any point during the hour.

A position counts in every hour it overlaps, so the series is a stock of open
exposure rather than a flow of closures. A trader holding two BTCUSDT
positions in the same hour shows 2.

Traders are generated as columns from stg_traders at compile time, so a renamed
trader changes the schema and needs a --full-refresh.
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

{%- set col_names = trader_column_names(traders, '_btc_count') -%}

WITH btcusdt_positions AS (
    SELECT
        trader_id,
        opened_at,
        closed_at
    FROM {{ ref('int_positions') }}
    WHERE symbol = 'BTCUSDT'
)

SELECT
    t.hour AS time,

    {%- for trader in traders %}

    COUNT(
        CASE WHEN p.trader_id = {{ trader['trader_id'] }} THEN 1 END
    ) AS "{{ col_names[loop.index0] }}"{% if not loop.last %},{% endif %}

    {%- endfor %}

FROM {{ ref('int_h1_timeline') }} t

LEFT JOIN btcusdt_positions p
    ON {{ positions_is_within_hour('t.hour', 'p.opened_at', 'p.closed_at') }}

GROUP BY t.hour
ORDER BY t.hour