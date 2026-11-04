--- CHECKING FOR NULL FIELDS
SELECT * FROM (
  SELECT * FROM (
    SELECT * FROM './dbt_project/seeds/positions.csv'
  ) AS _query
) AS _query
WHERE trader_id IS NULL OR 
  position_id IS NULL OR 
  market_type IS NULL OR 
  symbol IS NULL OR 
  side IS NULL OR 
  status IS NULL OR 
  is_fully_closed IS NULL OR 
  margin_mode IS NULL OR 
  leverage IS NULL OR 
  entry_price IS NULL OR 
  close_price IS NULL OR 
  opened_volume IS NULL OR 
  closed_volume IS NULL OR 
  remaining_volume IS NULL OR 
  max_open_interest IS NULL OR 
  gross_pnl IS NULL OR 
  funding_pnl IS NULL OR 
  trading_fee IS NULL OR 
  liquidation_fee IS NULL OR 
  net_pnl IS NULL OR 
  roi IS NULL OR 
  pnl_currency IS NULL OR 
  opened_at IS NULL OR 
  closed_at IS NULL OR 
  updated_at IS NULL;

--- COUNTING DISTINCT position_id
SELECT COUNT(DISTINCT(position_id)) FROM './dbt_project/seeds/positions.csv';

--- some position_id do repeat
WITH multiple_positions AS (
  SELECT DISTINCT(position_id) AS unique_positions, COUNT(*) AS count 
  FROM './dbt_project/seeds/positions.csv' 
  GROUP BY unique_positions 
  HAVING count > 1
)
SELECT * FROM './dbt_project/seeds/positions.csv'
WHERE position_id IN (SELECT unique_positions FROM multiple_positions)
ORDER BY position_id;
-- the above query leads to thinking that I need to add some surrogate key using dbt_utils.generate_surrogate_key 
-- and treat all rows from positons.csv and downstream model as unique