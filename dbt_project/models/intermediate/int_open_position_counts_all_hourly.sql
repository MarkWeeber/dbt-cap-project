{#
dbt_project/models/intermediate/int_open_position_counts_all_hourly.sql
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

{%- set col_names = trader_column_names(traders, '_count') -%}

WITH approved_positions AS (
    SELECT
        trader_id,
        opened_at,
        closed_at
    FROM {{ ref('int_positions') }}
)
SELECT
    t.hour AS time,

    {%- for trader in traders %}

    COUNT(
        CASE WHEN p.trader_id = {{ trader['trader_id'] }} THEN 1 END
    ) AS "{{ col_names[loop.index0] }}"{% if not loop.last %},{% endif %}

    {%- endfor %}

FROM {{ ref('int_h1_timeline') }} t

LEFT JOIN approved_positions p
    ON {{ positions_is_within_hour('t.hour', 'p.opened_at', 'p.closed_at') }}

GROUP BY t.hour
ORDER BY t.hour
