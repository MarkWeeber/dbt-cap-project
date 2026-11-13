{{ config(severity='error') }}

WITH per_symbol AS (
    {% for symbol in var('approved_symbols') %}
    SELECT '{{ symbol }}' AS symbol, count(*) AS bars,
           min("timestamp") AS first_bar, max("timestamp") AS last_bar
    FROM {{ ref('stg_' ~ symbol ~ '_h1') }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
    {% endfor %}
),
reference AS (
    SELECT max(bars) AS bars, min(first_bar) AS first_bar, max(last_bar) AS last_bar
    FROM per_symbol
)

SELECT p.symbol, p.bars, p.first_bar, p.last_bar
FROM per_symbol p
CROSS JOIN reference r
WHERE p.bars <> r.bars
   OR p.first_bar <> r.first_bar
   OR p.last_bar <> r.last_bar