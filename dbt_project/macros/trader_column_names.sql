{# dbt_project/macros/trader_column_names.sql #}
{#
  Builds the generated column list for the trader matrix models and fails
  compilation on a collision. Names are sanitised from trader names, which are
  free text, so collisions are possible and silent without this check.

  Returns a list of column names, ordered like the input rows.
#}
{% macro trader_column_names(traders, suffix) %}
    {%- set names = [] -%}
    {%- for trader in traders -%}
        {%- set cleaned = trader['name'] | trim | replace('"', '""')
                          | replace(' ', '_') | replace('__', '_') | replace('__', '_') -%}
        {%- set safe_name = cleaned if cleaned | length > 0 else 'trader_' ~ trader['trader_id'] -%}
        {%- do names.append(safe_name ~ suffix) -%}
    {%- endfor -%}
    {%- if names | length != names | unique | list | length -%}
        {{ exceptions.raise_compiler_error('duplicate generated column names: ' ~ names) }}
    {%- endif -%}
    {{ return(names) }}
{% endmacro %}