{#
    tests/assert_all_position_symbols_have_a_price_column.sql

    Fails when a symbol traded in int_positions has no matching *_close column
    in stg_close_h1, which would price it as NULL and drop it from aggregates.
#}

{%- set close_prices_rel = ref('stg_close_h1') -%}
{%- set positions_rel = ref('int_positions') -%}

{%- if execute -%}
    {%- set cols = adapter.get_columns_in_relation(close_prices_rel) | map(attribute='name') | list -%}
    {%- set symbols = [] -%}
    {%- for col in cols -%}
        {%- if col.endswith('_close') -%}{%- do symbols.append(col[:-6] | upper) -%}{%- endif -%}
    {%- endfor -%}
    {%- if symbols | length == 0 -%}
        {{ exceptions.raise_compiler_error('stg_close_h1 exposes no *_close columns') }}
    {%- endif -%}
{%- else -%}
    {%- set symbols = [] -%}
{%- endif -%}

{%- if symbols | length == 0 %}
SELECT NULL AS symbol WHERE FALSE
{%- else %}
SELECT DISTINCT symbol
FROM {{ positions_rel }}
WHERE UPPER(symbol) NOT IN ({{ "'" ~ symbols | join("', '") ~ "'" }})
{%- endif %}
