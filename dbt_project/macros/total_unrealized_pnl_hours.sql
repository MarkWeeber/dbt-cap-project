{#
  Macro: total_unrealized_pnl_hours
  Purpose: Mark-to-market value of a position at one hour, choosing the size to
           mark from the position's own state.

  Size selection:
    * CLOSED            -> opened_volume for the whole lifetime.
    * PARTIALLY_CLOSED  -> opened_volume up to the hour containing the partial
                           close, remaining_volume from that hour onwards.
    * anything else     -> opened_volume.

  The hour boundary is deliberate. The mark price is the close of the hour, so
  the hour carrying the partial close is already valued after the close; it must
  therefore also carry the reduced size, or price and volume would disagree.

  updated_at as the partial-close moment is an inference, not a broker field.
  The broker only sets closed_at on full closure. gross_pnl reproduces exactly
  from close_price and closed_volume, which proves a single close event, and
  updated_at is its only candidate timestamp. It has not moved in months, so it
  is a last-write mark rather than a funding heartbeat.

  Formulas:
    - Long:  (mark_price - entry_price) * volume
    - Short: (entry_price - mark_price) * volume

  Parameters:
    - side_col: Column holding 'LONG' or 'SHORT'.
    - entry_price_col: Column holding the position entry price.
    - opened_volume_col: Column holding the original position size.
    - remaining_volume_col: Column holding the size left after partial closes.
    - status_col: Column holding the position status.
    - updated_at_col: Column holding the last-write timestamp of the position record.
    - current_hour_col: Column holding the hour being valued.
    - mark_price_col: Column holding the symbol close price for the hour.
#}
{% macro total_unrealized_pnl_hours(
        side_col,
        entry_price_col,
        opened_volume_col,
        remaining_volume_col,
        status_col,
        updated_at_col,
        current_hour_col,
        mark_price_col) %}

    {%- set volume_expr -%}
        CASE
            WHEN {{ status_col }} = 'CLOSED' THEN {{ opened_volume_col }}
            WHEN {{ status_col }} = 'PARTIALLY_CLOSED'
                 AND {{ updated_at_col }} IS NOT NULL
                 AND date_trunc('hour', {{ updated_at_col }}) <= {{ current_hour_col }}
            THEN {{ remaining_volume_col }}
            ELSE {{ opened_volume_col }}
        END
    {%- endset -%}

    CASE
        WHEN UPPER({{ side_col }}) = 'LONG'
            THEN ({{ mark_price_col }} - {{ entry_price_col }}) * ({{ volume_expr }})
        WHEN UPPER({{ side_col }}) = 'SHORT'
            THEN ({{ entry_price_col }} - {{ mark_price_col }}) * ({{ volume_expr }})
        ELSE 0
    END
{% endmacro %}
