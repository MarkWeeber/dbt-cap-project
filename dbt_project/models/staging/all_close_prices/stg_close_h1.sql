{#
    Model: stg_close_h1
    Location: dbt_project/models/staging/all_close_prices/stg_close_h1.sql
    
    Purpose:
        Combines hourly close prices for all symbols listed in `approved_symbols`
        into a single wide table aligned by timestamp.
    
    Behavior:
        - Preserves every unique timestamp present in any source table.
        - Fills missing symbol prices for a given timestamp with NULL.
#}

{# 1. Fetch the list of configured trading pairs from dbt_project.yml #}
{%- set symbols = var('approved_symbols') -%}

WITH all_timestamps AS (
    {# 
        Build a master timestamp spine across all symbol tables.
        Using UNION (instead of UNION ALL) automatically deduplicates timestamps.
    #}
    {%- for symbol in symbols %}
        {%- set model_name = 'stg_' ~ symbol | lower ~ '_h1' %}
        SELECT "timestamp"
        FROM {{ ref(model_name) }}
        {%- if not loop.last %}
        UNION
        {%- endif %}
    {%- endfor %}
)

SELECT
    {# Master timestamp column #}
    spine."timestamp",

    {# Dynamically project each symbol's close price as <symbol>_close #}
    {%- for symbol in symbols %}
        {%- set sym_alias = symbol | lower %}
        {{ sym_alias }}."close" AS {{ sym_alias }}_close{%- if not loop.last %},{% endif %}
    {%- endfor %}

FROM all_timestamps AS spine

{# Left join every symbol table to the master spine to keep excess timestamps #}
{%- for symbol in symbols %}
    {%- set sym_alias = symbol | lower %}
    {%- set model_name = 'stg_' ~ sym_alias ~ '_h1' %}
LEFT JOIN {{ ref(model_name) }} AS {{ sym_alias }}
    ON spine."timestamp" = {{ sym_alias }}."timestamp"
{%- endfor %}

{# Optional: Sort chronologically for easier downstream inspection #}
ORDER BY spine."timestamp"
