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