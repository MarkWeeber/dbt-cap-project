{# 
  Macro: total_unrealized_pnl_hours
  Purpose: Calculates the unrealized PnL for an open trading position at a specific hour.
  Formulas:
    - Long:  (current_symbol_open_price - position_entry_price) * position_volume
    - Short: (position_entry_price - current_symbol_open_price) * position_volume
  Parameters:
    - side_col: Column representing position side ('LONG' or 'SHORT').
    - position_entry_price_col: Column representing the position's entry price.
    - volume_col: Column representing the position's opened volume.
    - symbol_current_price_col: Column representing the symbol's open price for the given hour.
    - closed_at_time: Column representing when the position was closed.
    - current_hour: Column or timestamp representing the current hour window.
    - gross_pnl: Column representing the realized gross PnL if closed.
#}
{% macro total_unrealized_pnl_hours(side_col, position_entry_price_col, volume_col, symbol_current_price_col, closed_at_time, current_hour, gross_pnl) %} 
    CASE
        WHEN UPPER({{ side_col }}) = 'LONG' 
            THEN ({{ symbol_current_price_col }} - {{ position_entry_price_col }}) * {{ volume_col }}
        WHEN UPPER({{ side_col }}) = 'SHORT' 
            THEN ({{ position_entry_price_col }} - {{ symbol_current_price_col }}) * {{ volume_col }}
        ELSE 0
END
{% endmacro %}

{#
    CASE 
        WHEN  {{ closed_at_time }} IS NOT NULL AND {{ closed_at_time }} < {{ current_hour }} + INTERVAL '1 hour'
            THEN {{ gross_pnl }}
        ELSE
    END
#}