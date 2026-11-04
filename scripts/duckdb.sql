--- LIST SCHEMAS
SHOW SCHEMAS;

--- LIST TABLES
SHOW ALL TABLES;

--- SOME TABLES
SELECT * FROM RAW.RAW_BTCUSDT_D1 LIMIT 10;

SELECT * FROM DEV.BTCUSDT_D1 LIMIT 10;

--- DROPPING WHOLE DEV
DROP SCHEMA DEV CASCADE;

--- SOME TESTS
SELECT * FROM RAW.RAW_BTCUSDT_D1 WHERE Open IS NULL;

SELECT * FROM RAW.RAW_XRPUSDT_H1 WHERE high IS NULL;

SELECT * FROM "dev"."RAW"."RAW_XRPUSDT_H1" LIMIT 1000

--- TESTING THE JOIN OF positions and open_positions
SELECT * FROM "dev"."DEV"."open_positions" AS op INNER JOIN "dev"."DEV"."positions" as p ON op.trader_id = p.trader_id ORDER BY op.trader_id LIMIT 10;

--- counting trader's overall open positions
SELECT trader_id, COUNT(*) AS count FROM "dev"."DEV"."open_positions" GROUP BY trader_id ORDER BY count; 

--- fidning trader's overall history positions
SELECT trader_id, COUNT(*) AS count FROM "dev"."DEV"."positions" GROUP BY trader_id ORDER BY count; 

--- SOME CTEs
WITH trader_with_least_history_trades AS (
    SELECT trader_id, COUNT(*) AS count FROM "dev"."DEV"."positions" GROUP BY trader_id ORDER BY count LIMIT 1
),
trader_with_least_open_trades AS (
    SELECT trader_id, COUNT(*) AS count FROM "dev"."DEV"."open_positions" GROUP BY trader_id ORDER BY count LIMIT 1
),
all_trades AS (
    SELECT * FROM "dev"."DEV"."open_positions" AS op FULL OUTER JOIN "dev"."DEV"."positions" as p ON op.trader_id = p.trader_id ORDER BY op.trader_id
)
SELECT * FROM all_trades WHERE trader_id = (SELECT trader_id FROM trader_with_least_open_trades);

--- 

SELECT * FROM (
  SELECT * FROM './dbt_project/seeds/positions.csv'
) AS _query
WHERE "is_fully_closed" IN ('false')

--- testing combining orders and positions tables
SELECT 
    o.*
FROM "dev"."DEV"."orders" o
JOIN "dev"."DEV"."positions" p
    ON o.trader_id = p.trader_id
   AND o.symbol = p.symbol
   -- fitting into time window
   AND o.created_at_ts >= p.opened_at_ts
   AND o.created_at_ts <= COALESCE(p.closed_at_ts, p.updated_at)
WHERE p.trader_id = 4555301969096159497
  AND p.position_id = 1127582159478145024;


--- testing some orders.csv 
with b as (
    select min(created_at) as mn, max(updated_at) as mx, count(*) as n
    from 'dbt_project/seeds/orders.csv'
)
select *
from b
where n > 0
  and (mx - mn) < 20 * 86400000;

--- SOME TESTS FOR CLEAN UPS:
SELECT * FROM 'dbt_project/seeds/positions.csv'
WHERE closed_at IS NULL;