{%- set symbol_filter = 'BTCUSDT' -%}
{# Extract base asset in lowercase (e.g., BTCUSDT -> btc) #}
{%- set symbol_prefix = symbol_filter | replace('USDT', '') | lower -%}

{# Step 1: Query to fetch all unique traders (ID and name) from the traders table #}
{%- set get_traders_query -%}
    SELECT trader_id, name 
    FROM {{ ref('traders') }}
    WHERE trader_id IS NOT NULL
    ORDER BY trader_id
{%- endset -%}

{%- set results = run_query(get_traders_query) -%}

{# Step 2: Safely extract rows during compile phase, fallback to empty list if not executing #}
{%- if execute -%}
    {%- set traders = results.rows -%}
{%- else -%}
    {%- set traders = [] -%}
{%- endif -%}

WITH h1_timeline AS (
    {# Step 3: Extract hourly timestamps and symbol open prices from the hourly source #}
    SELECT 
        timestamp AS h_time,
        open AS symbol_open_price
    FROM {{ ref('src_btcusdt_h1') }}
),
filtered_positions AS (
    {# Step 4: Pre-filter positions by symbol and select necessary financial/timing fields #}
    SELECT 
        trader_id,
        side,
        entry_price,
        opened_volume,
        opened_at,
        closed_at
    FROM {{ ref('int_positions') }}
    WHERE symbol = '{{ symbol_filter }}'
)
SELECT 
    t.h_time AS time,
    
    {# Step 5: Dynamically iterate through traders and generate formatted PnL columns #}
    {%- for trader in traders %}
    
    {# Format column name: escape quotes, replace spaces with underscores, and append _btc_pnl #}
    {%- set safe_name = trader['name'] | replace('"', '""') | replace(' ', '_') -%}
    {%- set col_name = safe_name ~ '_' ~ symbol_prefix ~ '_pnl' -%}
    
    SUM(
        CASE WHEN p.trader_id = {{ trader['trader_id'] }} 
             AND {{ positions_is_within_hour('t.h_time', 'p.opened_at', 'p.closed_at') }} 
             THEN {{ total_unrealized_pnl_hours('p.side', 'p.entry_price', 'p.opened_volume', 't.symbol_open_price') }}
             ELSE 0 END
    ) AS "{{ col_name }}"{% if not loop.last %},{% endif %}
    
    {%- endfor %}

FROM h1_timeline t

{# Step 6: Join timeline with positions using the interval overlap condition #}
LEFT JOIN filtered_positions p 
    ON p.opened_at < t.h_time + INTERVAL '1 hour' 
    AND (p.closed_at > t.h_time OR p.closed_at IS NULL)

GROUP BY t.h_time, t.symbol_open_price
ORDER BY t.h_time