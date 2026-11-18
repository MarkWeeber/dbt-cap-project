{#
    dbt_project/tests/assert_no_missing_hourly_close.sql
#}

{%- set close_prices_rel = ref('stg_close_h1') -%}

{%- if execute -%}
    {%- set cols = adapter.get_columns_in_relation(close_prices_rel) | map(attribute='name') | list -%}
    {%- set price_cols = [] -%}
    {%- for col in cols -%}
        {%- if col.endswith('_close') -%}{%- do price_cols.append(col) -%}{%- endif -%}
    {%- endfor -%}
    {%- if price_cols | length == 0 -%}
        {{ exceptions.raise_compiler_error('stg_close_h1 exposes no *_close columns') }}
    {%- endif -%}
{%- else -%}
    {%- set price_cols = [] -%}
{%- endif -%}

{%- for col in price_cols %}
SELECT "timestamp", '{{ col[:-6] | upper }}' AS symbol
FROM {{ close_prices_rel }}
WHERE {{ col }} IS NULL
{%- if not loop.last %}
UNION ALL
{%- endif %}
{%- endfor %}
