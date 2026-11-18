{#
    dbt_project/tests/assert_close_h1_spans_window.sql

    Fails when stg_close_h1 does not cover the configured window hour by hour.
    The table is one row per hour with one column per symbol, so the expected
    row count equals the expected hour count.

    Literals are cast to TIMESTAMPTZ explicitly: the column carries a time zone,
    and a bare TIMESTAMP literal would be resolved against the session zone.
#}

{%- set d_from = modules.datetime.datetime.strptime(var('date_from'), '%Y-%m-%d') -%}
{%- set d_to = modules.datetime.datetime.strptime(var('date_to'), '%Y-%m-%d') -%}
{%- set expected_hours = (d_to - d_from).days * 24 + 24 -%}

{%- set expected_first = "CAST('" ~ var('date_from') ~ " 00:00:00+00' AS TIMESTAMPTZ)" -%}
{%- set expected_last = "CAST('" ~ var('date_to') ~ " 23:00:00+00' AS TIMESTAMPTZ)" -%}

WITH bounds AS (
    SELECT
        min("timestamp") AS first_hour,
        max("timestamp") AS last_hour,
        count(*) AS n
    FROM {{ ref('stg_close_h1') }}
)

SELECT
    first_hour, last_hour, n,
    {{ expected_first }} AS expected_first,
    {{ expected_last }} AS expected_last,
    {{ expected_hours }} AS expected_n
FROM bounds
WHERE first_hour <> {{ expected_first }}
   OR last_hour <> {{ expected_last }}
   OR n <> {{ expected_hours }}
