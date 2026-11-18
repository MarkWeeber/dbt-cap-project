{# tests/assert_approved_symbols_match_stg_symbols.sql #}

{%- set symbols = var('approved_symbols') -%}

SELECT
    COALESCE(v.symbol, s.symbol) AS symbol,
    CASE
        WHEN s.symbol IS NULL THEN 'in var, missing from seed'
        ELSE 'in seed, missing from var'
    END AS problem
FROM (
    SELECT UPPER(symbol) AS symbol
    FROM unnest({{ symbols }}) AS t(symbol)
) v
FULL OUTER JOIN (
    SELECT UPPER(symbol) AS symbol FROM {{ ref('symbols') }}
) s
    ON s.symbol = v.symbol
WHERE v.symbol IS NULL OR s.symbol IS NULL
