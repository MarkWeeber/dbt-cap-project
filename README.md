A capstone dbt project

# Binance Trader Performance Analytics — dbt + DuckDB

Analytics engineering capstone: reconstructing per-trader performance for top Binance
futures traders from public leaderboard data — and verifying the result against the
metrics the data source publishes itself.

> **Status:** work in progress. Extract layer done, staging and marts under construction.

## The brief

Copy-trading leaderboards rank traders by PnL. A single number hides a lot: how much of
the profit came from a handful of lucky trades, how deep the account went underwater,
how much of the "profit" was eaten by fees and funding.

The goal is to rebuild that picture from raw position data — hourly, per position, per
trader — and prove the numbers are right.

## Data source

- **Kopyon API** (Binance Smart Money leaderboard) — public trader rankings, closed
  position history, trader profiles, daily PnL / ROI / balance series.
  20 traders, all symbols, 2026-01-01 → 2026-09-30.
- **Binance public klines API** — hourly OHLCV for 6 symbols (BTC, ETH, SOL, XRP, DOGE, BNB).

Both sources are public and the extraction is reproducible from a pinned date window.

## Approach

    raw API + OHLCV  →  seeds / sources  →  staging  →  intermediate  →  marts

Staging is 1:1 with the raw layer (cast, rename, dedupe). Business logic lives in
intermediate. Marts hold the facts and dimensions.

## Correctness

Four accounting formulas are reproduced exactly from raw position data:

| Metric | Formula |
| --- | --- |
| `gross_pnl` | `(close_price − entry_price) × closed_volume × sign(side)` |
| `net_pnl` | `gross_pnl − trading_fee + funding_pnl − liquidation_fee` |
| `margin` | `entry_price × closed_volume / leverage` |
| `roi` | `net_pnl / margin` |

Recomputed values are reconciled against the source's own published daily PnL series
(one point per day per trader). Matching an external system — not just being
self-consistent — is the point.

## Findings

_To be filled in once the marts are built._

## Stack

`dbt-core` + `dbt-duckdb`, DuckDB, Python 3, `dbt_utils`, `dbt_expectations`.

## Roadmap

- [x] API reconnaissance: routes, pagination, rate limits, history depth
- [ ] Config layer + repo hygiene
- [ ] Extraction scripts (window chunking, cursor pagination)
- [ ] Seeds and source definitions with tests
- [ ] Staging layer
- [ ] Position-level PnL recomputation + reconciliation tests
- [ ] Daily reconciliation against the source's published series
- [ ] Hourly mark-to-market equity curve
- [ ] `dbt docs` + GitHub Action

## Running it

    uv venv && uv pip install -r requirements.txt
    export KOPYON_API_KEY=...
    python scripts/extract_data.py
    python scripts/extract_traders.py
    python scripts/build_seeds.py
    dbt deps && dbt build