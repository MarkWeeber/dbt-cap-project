{# 
  Macro: positions_is_within_hour
  Purpose: Checks if a trading position is active during a given hour.
  Logic: Uses strict inequalities (< and >) to correctly capture short intra-hour 
         trades and boundary conditions (when trades open/close right on the hour mark).
  Parameters:
    - hour_col: The timestamp representing the beginning of the hour.
    - opened_col: The timestamp when the position was opened.
    - closed_col: The timestamp when the position was closed (can be NULL if still open).
#}
{% macro positions_is_within_hour(hour_col, opened_col, closed_col) %}
    {{ opened_col }} < {{ hour_col }} + INTERVAL '1 hour'
    AND
    (
        {{ closed_col }} > {{ hour_col }} OR {{ closed_col }} IS NULL
    )
{% endmacro %}