{#
    Macro: get_column_names
    Location: dbt_project/macros/get_column_names.sql

    Arguments:
        relation (Relation): A dbt Relation object passed via ref('...') or source('...', '...').
        skip_col (string, optional): A column name to exclude from the returned list.

    Returns:
        list[string]: A list of lowercase column names.
#}

{% macro get_column_names(relation, skip_col=none) %}

    {%- if execute -%}
        {%- set cols = adapter.get_columns_in_relation(relation) -%}
        {%- set col_names = cols | map(attribute='name') | map('lower') | list -%}

        {%- if skip_col is not none -%}
            {%- set col_names = col_names | reject('equalto', skip_col | lower) | list -%}
        {%- endif -%}

        {{ return(col_names) }}
    {%- else -%}
        {{ return([]) }}
    {%- endif -%}

{% endmacro %}
