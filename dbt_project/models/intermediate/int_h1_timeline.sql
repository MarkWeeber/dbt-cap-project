{#
    dbt_project/models/intermediate/int_h1_timeline.sql
    
    Every hourly bar timestamp present in any approved symbol. A union rather
    than one symbol's series, so a gap in a single symbol cannot erase the hour
    for all of them.

    The list comes from a var, not from stg_symbols, so the refs resolve during
    parsing and the dependency edges exist. Divergence from stg_symbols is
    caught by assert_approved_symbols_match_stg_symbols.
#}

{%- set symbols = var('approved_symbols') -%}

WITH all_bars AS (
    {%- for symbol in symbols %}
    SELECT "timestamp" FROM {{ ref('stg_' ~ symbol | lower ~ '_h1') }}
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
    {%- endfor %}
)

SELECT DISTINCT "timestamp" AS hour
FROM all_bars
