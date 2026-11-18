{# 
  Macro: positions_is_within_hour
  Purpose: Predicate answering "was this position still open during this hour?". Used
           in the join that pairs every position with the hours it overlaps.

  Interval convention: [hour, hour + 1) - half open.

  Boundary behaviour, deliberate:
    * A position opening exactly on the hour mark is excluded from that hour.
    * A position closing exactly on the hour mark is also excluded, so the hour
      carrying the close is the one before it, whose mark price is the close
      tick itself.
    * closed_col NULL (still open) matches every hour from opened_at onwards.

  Parameters:
    hour_col:   timestamp marking the start of the hour interval.
    opened_col: position open timestamp.
    closed_col: position close timestamp, NULL while the position is open.
#}
{% macro positions_is_within_hour(hour_col, opened_col, closed_col) %}
    {{ opened_col }} < {{ hour_col }} + INTERVAL '1 hour'
    AND
    (
        {{ closed_col }} > {{ hour_col }} OR {{ closed_col }} IS NULL
    )
{% endmacro %}
