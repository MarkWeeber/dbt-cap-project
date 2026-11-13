SELECT t.name, p.* FROM
  "dev"."DEV"."int_positions" AS p
  INNER JOIN "dev"."DEV"."stg_traders" AS t ON p.trader_id = t.trader_id
  WHERE p.symbol = 'BTCUSDT' AND t.name = 'r2d20'
ORDER BY p.trader_id DESC, p.opened_at ASC
