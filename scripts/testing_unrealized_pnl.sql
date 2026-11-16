SELECT * FROM (
  SELECT * FROM "dev"."DEV"."int_unrealized_pnl_btc_hourly"
) AS _query
WHERE "time" BETWEEN '2026-06-17' AND '2026-06-27'


-- SELECT * FROM (
--   SELECT * FROM "dev"."DEV"."int_positions"
-- ) AS _query
-- WHERE "opened_at" BETWEEN '2026-06-17' AND '2026-06-26' AND "trader_id" IN ('4555302040718094637') AND "symbol" = 'AAVEUSDT'