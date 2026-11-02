# Binance Trader Performance Analytics — dbt + DuckDB

Analytics engineering capstone: reconstructing per-trader performance for top Binance
futures traders from public leaderboard data — and verifying the result against the
metrics the data source publishes itself.

> **Status:** extract and seed layers done. Staging and marts under construction.

## The brief

Copy-trading leaderboards rank traders by PnL. A single number hides a lot: how much of
the profit came from a handful of lucky trades, how deep the account went underwater,
how much of the "profit" was eaten by fees and funding.

The goal is to rebuild that picture from raw position data — hourly, per position, per
trader — and prove the numbers are right.

## Data source

- **Kopyon API** (Binance Smart Money leaderboard) — public trader rankings, closed
  position history, trader profiles, daily PnL / ROI / balance series.
  20 traders, all symbols, window **2026-01-01 → 2026-09-30**.
- **Binance public klines API** — hourly OHLCV for 8 symbols (BTC, ETH, SOL, AAVE, TWT,
  BNB, LTC, XRP, all USDT-quoted).

Both sources are public and the extraction is reproducible from a pinned date window.

**Snapshot taken: `2026-10-28T12:55:17Z`** — the moment the last successful extraction
ran. The source exposes trader profiles and open positions as live state, not history,
so those two seeds are cross-sectional and carry the timestamp in a `snapshot_at`
column. Windowed seeds carry no such column, by design.

## Scope decisions

### Open positions are a separate layer, not a join target

The source serves open positions from an endpoint that shares no key with position
history:

- **No identity.** History rows carry `positionId`; open-position rows do not — only
  symbol, side and entry price, so any link is a heuristic over a float.
- **Different grain.** A history row is a realised episode; an open row is a live
  remainder. A partially closed position exists in both, split across the two
  endpoints, so a join yields 1:1 rows that must be de-duplicated and a double-counted
  margin if summed naively.
- **Different time semantics.** History is filtered by close/update time inside the
  window; the snapshot is unwindowed and its response carries no timestamp at all.
  A time join is meaningless, identity is unreliable.
- **Tiny overlap.** 80 open rows across 20 traders, 3 of which intersect history, 13 in
  approved symbols.

Joining them would add an unverifiable step to a project whose whole value is
verification. The snapshot therefore stays a dated cross-sectional layer — margin load,
distance to liquidation, concentration, unrealised share — and is never mixed into
windowed facts.

Doing it properly requires scheduled snapshots plus an SCD2 over positions: an iteration
on top of the current window, not a missing piece.

### Approved symbols

Hourly mark-to-market needs candles, so it is restricted to an approved symbol list in
`seeds/symbols.csv`. Position-level analysis (PnL recomputation, trader metrics,
reconciliation) runs over every symbol regardless. Coverage of the approved list is
25.6% of positions and is reported alongside every MTM figure rather than hidden.

### Known data gaps

Two of twenty traders have a small gap (single-digit rows) where the upstream refuses to
serve specific records; this is reproducible and documented rather than patched over.
Two traders expose no orders at all.

## Approach

    raw API + OHLCV  →  seeds / sources  →  staging  →  intermediate  →  marts

Staging is 1:1 with the raw layer (cast, rename, dedupe). Business logic lives in
intermediate. Marts hold the facts and dimensions.

Surrogate keys are generated in dbt with `dbt_utils.generate_surrogate_key`, not in
Python: the same `positionId` genuinely appears twice with opposite `side`, so the
broker id alone is not unique.

## Correctness

Four accounting formulas are reproduced exactly from raw position data:

| Metric | Formula |
| --- | --- |
| `gross_pnl` | `(close_price − entry_price) × closed_volume × sign(side)` |
| `net_pnl` | `gross_pnl − trading_fee + funding_pnl − liquidation_fee` |
| `margin` | `entry_price × closed_volume / leverage` |
| `roi` | `net_pnl / margin` |

Recomputed values are reconciled against the source's own published daily PnL series
(one point per day per trader, cumulative and period-relative, so compared by
differencing). Matching an external system — not just being self-consistent — is the
point.

## Findings

_To be filled in once the marts are built._

## Stack

`dbt-core` + `dbt-duckdb`, DuckDB 1.3.x (pinned — the VS Code extension cannot read the
2.x storage format), Python 3, `dbt_utils`, `dbt_expectations`.

## Roadmap

- [x] API reconnaissance: routes, pagination, rate limits, history depth
- [x] Config layer + repo hygiene
- [x] Extraction scripts (window chunking, cursor pagination, page-size fallback)
- [x] Seeds with snapshot dating
- [ ] Source definitions and seed tests
- [ ] Staging layer
- [ ] Position-level PnL recomputation + reconciliation tests
- [ ] Daily reconciliation against the source's published series
- [ ] Hourly mark-to-market equity curve
- [ ] `dbt docs` + GitHub Action

## Running it

    uv venv && uv pip install -r requirements.txt
    export KOPYON_API_KEY=...

    make build            # full refresh: APIs, seeds, load, dbt build
    make build-offline    # committed parquet and seeds only, no API key
    make rebuild          # wipe DuckDB and rebuild offline
    make db               # SQL shell on the dev database