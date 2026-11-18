{#
dbt_project/models/intermediate/int_unrealized_pnl_hourly.sql

One row per hour of the analysis window, one column per trader, holding that
trader's aggregate unrealized PnL across every position open during the hour,
over all approved symbols.

Mark price is the close of the hour for the position's own symbol, so a position
opened part-way through an hour is valued after its entry rather than before it.

Sign convention follows the broker: positive means the position is in profit.

A partially closed position is marked on its remaining size from the hour of the
partial close onwards; its realized leg stays in net_pnl and does not appear
here, so the column drops sharply at that hour by design.

Traders are generated as columns from stg_traders at compile time, so a renamed
trader changes the schema and needs a --full-refresh. The symbol-to-column
mapping is read from stg_close_h1, so a symbol without a *_close column prices as
NULL and is caught by assert_all_position_symbols_have_a_price_column.
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

{%- set col_names = trader_column_names(traders, '_unrealized_pnl') -%}

{%- set close_prices_rel = ref('stg_close_h1') -%}

{%- set all_cols = get_column_names(close_prices_rel, 'timestamp') -%}
{%- set symbol_cols = [] -%}
{%- set ignored_cols = [] -%}
{%- for col in all_cols -%}
    {%- if col.endswith('_close') -%}
        {%- do symbol_cols.append(col) -%}
    {%- else -%}
        {%- do ignored_cols.append(col) -%}
    {%- endif -%}
{%- endfor -%}

{%- if execute -%}
    {%- if symbol_cols | length == 0 -%}
        {{ exceptions.raise_compiler_error(
            'stg_close_h1 exposes no *_close columns; found: ' ~ all_cols) }}
    {%- endif -%}
    {%- if ignored_cols | length > 0 -%}
        {{ log('stg_close_h1 columns ignored, not *_close: ' ~ ignored_cols, info=True) }}
    {%- endif -%}
{%- endif -%}

{%- set dynamic_close_price_sql -%}
    CASE UPPER(p.symbol)
    {%- for col in symbol_cols %}
        WHEN '{{ col[:-6] | upper }}' THEN t."{{ col }}"
    {%- endfor %}
    ELSE NULL
    END
{%- endset -%}

WITH positions AS (
    SELECT
        trader_id,
        symbol,
        side,
        entry_price,
        opened_volume,
        remaining_volume,
        status,
        updated_at,
        opened_at,
        closed_at
    FROM {{ ref('int_positions') }}
),

positions_priced AS (
    SELECT
        p.trader_id,
        p.side,
        p.entry_price,
        p.opened_volume,
        p.remaining_volume,
        p.status,
        p.updated_at,
        t."timestamp" AS hour,
        {{ dynamic_close_price_sql }} AS mark_price
    FROM {{ close_prices_rel }} AS t
    LEFT JOIN positions AS p
        ON {{ positions_is_within_hour('t."timestamp"', 'p.opened_at', 'p.closed_at') }}
)

SELECT
    hour AS time
    {%- for trader in traders %},

    SUM(
        CASE WHEN trader_id = {{ trader['trader_id'] }}
             THEN {{ total_unrealized_pnl_hours(
                        'side', 'entry_price', 'opened_volume', 'remaining_volume',
                        'status', 'updated_at', 'hour', 'mark_price') }}
             ELSE 0 END
    ) AS "{{ col_names[loop.index0] }}"
    {%- endfor %},

    SUM(CASE WHEN mark_price IS NULL AND trader_id IS NOT NULL THEN 1 ELSE 0 END) AS missing_marks

FROM positions_priced
GROUP BY hour
ORDER BY hour
