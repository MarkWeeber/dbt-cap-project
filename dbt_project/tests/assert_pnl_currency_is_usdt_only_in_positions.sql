{{ config(severity='error') }}

SELECT * FROM {{ref('stg_positions')}}
WHERE symbol IN (SELECT symbol FROM {{ref('stg_symbols')}}) AND pnl_currency IS DISTINCT FROM 'USDT'
