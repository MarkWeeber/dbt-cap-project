#!/usr/bin/env python3
"""
Build dbt CSV seeds from the raw trader JSON under data/raw/traders.

Stage 2 of 2 of the trader-seed pipeline. Reads the JSON produced by
extract_traders.py and writes one CSV per seed entity, with an explicit grain
declared for each. Every output file is overwritten on each run, so the seeds
always mirror the current raw extract.

Notes on field availability
---------------------------
The REST `orderHistory` route returns only a subset of order attributes
(symbol, side, positionSide, price, quantity, quoteQuantity, createdAt,
updatedAt). Fields such as orderId, positionId, action, orderType, status,
leverage and realizedPnl are NOT present, so:
  - `order_id`   is synthesised from stable order attributes;
  - `position_id` and `leverage` are recovered by matching each order to the
    position whose [openedAt, closedAt] window contains it.
Both are documented as derived, not exchange-provided.
"""

import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# =============================================================================
# CONFIGURATION - modify these values
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOURCE_PATH = PROJECT_ROOT / "data" / "raw" / "traders"
SEEDS_PATH = PROJECT_ROOT / "dbt_project" / "seeds"

QUOTE_ASSETS = ["USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USD"]
CONTRACT_TYPE = "PERPETUAL"

# Tolerance when matching an order timestamp to a position window (ms).
MATCH_TOLERANCE_MS = 60_000

# Candidate key names, checked in the object root and inside sourceDetails.
ORDER_KEYS = {
    "order_id": ["orderId", "id", "order_id"],
    "position_id": ["positionId", "position_id"],
    "action": ["action"],
    "order_type": ["orderType", "type", "order_type"],
    "status": ["status"],
    "leverage": ["leverage"],
    "realized_pnl": ["realizedPnl", "realized_pnl"],
    "trading_fee": ["tradingFee", "trading_fee"],
}


# =============================================================================
# HELPERS
# =============================================================================

def load_json(path: Path):
    """Read a JSON file, returning {} when it is missing or empty."""
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as fh:
        return json.load(fh) or {}


def pick_multi(obj: dict, keys: list[str], default=None):
    """
    Read the first present key from the object root, then from sourceDetails.

    Kopyon nests exchange-specific fields inside `sourceDetails`, and the exact
    key name can vary, so several candidates are tried in order.
    """
    src = obj.get("sourceDetails") or {}
    for key in keys:
        if obj.get(key) is not None:
            return obj[key]
    for key in keys:
        if src.get(key) is not None:
            return src[key]
    return default


def pick(obj: dict, key: str, default=None):
    """Single-key convenience wrapper around pick_multi."""
    return pick_multi(obj, [key], default)


def to_ms(value):
    """Normalise a ms epoch or ISO timestamp string to an integer ms epoch."""
    if value in (None, "", 0):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def ms_to_iso(value):
    """Convert a millisecond epoch (or ISO string) to a UTC ISO timestamp."""
    ms = to_ms(value)
    return None if ms is None else datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def split_symbol(symbol: str):
    """Split 'BTCUSDT' into ('BTC', 'USDT'); falls back to ('', symbol)."""
    for quote in QUOTE_ASSETS:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)], quote
    return "", symbol


def write_csv(filename: str, columns: list[str], rows: list[dict]) -> int:
    """Write one seed CSV with a fixed column order. Returns the row count."""
    path = SEEDS_PATH / filename
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col) for col in columns})
    print(f"  {filename}: {len(rows)} rows")
    return len(rows)


def iter_trader_folders():
    """Yield (folder, trader_id) for every extracted trader folder."""
    if not SOURCE_PATH.exists():
        return
    for folder in sorted(SOURCE_PATH.iterdir()):
        if not folder.is_dir() or folder.name.startswith("_"):
            continue
        yield folder, folder.name.rsplit("_", 1)[-1]


_MANIFEST = None


def manifest() -> dict:
    """Lazily load the extraction manifest (used for run metadata columns)."""
    global _MANIFEST
    if _MANIFEST is None:
        _MANIFEST = load_json(SOURCE_PATH / "_manifest.json")
    return _MANIFEST


def load_profiles() -> dict[str, dict]:
    """Return {trader_id: profile_json} for every extracted trader."""
    return {tid: load_json(folder / "profile.json") for folder, tid in iter_trader_folders()}


def load_positions_index() -> dict[tuple[str, str], list[dict]]:
    """Return {(trader_id, symbol): [positions]} used for order->position matching."""
    index = defaultdict(list)
    for folder, trader_id in iter_trader_folders():
        for path in folder.glob("positions_*.json"):
            for pos in load_json(path).get("positions", []):
                index[(trader_id, pos.get("symbol"))].append(pos)
    return index


def match_position(order: dict, candidates: list[dict]):
    """
    Find the position whose open/close window contains the order timestamp.

    Returns the matched position dict, or None. A small tolerance is applied
    because the exchange stamps `openedAt` at the first fill, which can be a
    fraction of a second after the order itself.
    """
    created = to_ms(pick(order, "createdAt"))
    if created is None:
        return None
    for pos in candidates:
        opened = to_ms(pick(pos, "openedAt"))
        closed = to_ms(pick(pos, "closedAt"))
        if opened and closed and (opened - MATCH_TOLERANCE_MS) <= created <= (closed + MATCH_TOLERANCE_MS):
            return pos
    return None


# =============================================================================
# SEED BUILDERS
# =============================================================================

def build_traders() -> list[dict]:
    """Seed: raw_traders. Grain: one row per trader."""
    rows = []
    for folder, trader_id in iter_trader_folders():
        profile = load_json(folder / "profile.json")
        details = profile.get("sourceDetails", {}) or {}
        rows.append({
            "trader_id": trader_id,
            "trader_name": profile.get("name"),
            "is_ai": details.get("isAi"),
            "avatar_url": profile.get("avatarUrl"),
            "account_name": details.get("accountName"),
            "subscribers_at_extract": details.get("subscribers"),
            "extracted_at": manifest().get("extracted_at"),
        })
    return rows


def build_symbols() -> list[dict]:
    """Seed: raw_symbols. Grain: one row per symbol x market type."""
    seen = {}
    for folder, _ in iter_trader_folders():
        for path in folder.glob("positions_*.json"):
            market_type = path.stem.split("_")[-1]
            for pos in load_json(path).get("positions", []):
                symbol = pos.get("symbol")
                if symbol and symbol not in seen:
                    base, quote = split_symbol(symbol)
                    seen[symbol] = {
                        "symbol": symbol,
                        "base_asset": base,
                        "quote_asset": quote,
                        "market_type": market_type,
                        "contract_type": CONTRACT_TYPE,
                    }
    return list(seen.values())


def build_closed_positions() -> list[dict]:
    """Seed: raw_closed_positions. Grain: one row per closed position event."""
    rows = []
    for folder, trader_id in iter_trader_folders():
        for path in folder.glob("positions_*.json"):
            market_type = path.stem.split("_")[-1]
            for pos in load_json(path).get("positions", []):
                rows.append({
                    "position_id": pick(pos, "positionId"),
                    "trader_id": trader_id,
                    "symbol": pos.get("symbol"),
                    "market_type": market_type,
                    "side": pos.get("side"),
                    "margin_mode": pick(pos, "marginMode"),
                    "leverage": pick(pos, "leverage"),
                    "opened_at_utc": ms_to_iso(pick(pos, "openedAt")),
                    "closed_at_utc": ms_to_iso(pick(pos, "closedAt")),
                    "entry_price": pick(pos, "entryPrice"),
                    "close_price": pick(pos, "closePrice"),
                    "opened_volume": pick(pos, "openedVolume"),
                    "closed_volume": pick(pos, "closedVolume"),
                    "gross_pnl": pick(pos, "grossPnl"),
                    "trading_fee": pick(pos, "tradingFee"),
                    "funding_fee": pick(pos, "fundingPnl"),
                    "liquidation_fee": pick(pos, "liquidationFee"),
                    "net_pnl": pick(pos, "netPnl"),
                    "pnl_currency": pick(pos, "pnlCurrency"),
                    "status": pick(pos, "status"),
                    "extracted_at": manifest().get("extracted_at"),
                })
    return rows


def build_orders() -> list[dict]:
    """
    Seed: raw_orders. Grain: one row per executed order.

    order_id is synthesised (plus a counter for uniqueness); position_id and
    leverage are recovered by time-matching against positions.
    """
    rows = []
    positions_index = load_positions_index()

    for folder, trader_id in iter_trader_folders():
        for path in folder.glob("orders_*.json"):
            market_type = path.stem.split("_")[-1]
            for seq, order in enumerate(load_json(path).get("orders", []), 1):
                created = ms_to_iso(pick(order, "createdAt"))
                symbol = order.get("symbol")

                matched = match_position(order, positions_index.get((trader_id, symbol), []))

                order_id = pick_multi(order, ORDER_KEYS["order_id"]) or "|".join(str(v) for v in [
                    trader_id, symbol, created, pick(order, "price"), pick(order, "quantity"), seq,
                ])

                rows.append({
                    "order_id": order_id,
                    "position_id": (pick_multi(order, ORDER_KEYS["position_id"])
                                    or pick(matched, "positionId") if matched else None),
                    "trader_id": trader_id,
                    "symbol": symbol,
                    "side": order.get("side"),
                    "position_side": order.get("positionSide"),
                    "action": pick_multi(order, ORDER_KEYS["action"]),
                    "order_type": pick_multi(order, ORDER_KEYS["order_type"]),
                    "status": pick_multi(order, ORDER_KEYS["status"]),
                    "price": pick(order, "price"),
                    "quantity": pick(order, "quantity"),
                    "quote_quantity": pick(order, "quoteQuantity"),
                    "leverage": (pick_multi(order, ORDER_KEYS["leverage"])
                                 or pick(matched, "leverage") if matched else None),
                    "realized_pnl": pick_multi(order, ORDER_KEYS["realized_pnl"]),
                    "trading_fee": pick_multi(order, ORDER_KEYS["trading_fee"]),
                    "created_at_utc": created,
                    "updated_at_utc": ms_to_iso(pick(order, "updatedAt")),
                })
    return rows


def build_performance_actual() -> list[dict]:
    """
    Seed: raw_trader_performance_actual. Grain: trader x period x type x snapshot.

    roi/pnl/rank come from the ranking; win_rate/max_drawdown/days_active/
    net_transfer come from each trader's profile (they are absent from the
    ranking payload).
    """
    rankings = load_json(SOURCE_PATH / "_ranking.json")
    profiles = load_profiles()
    rows = []
    for key, traders in rankings.items():
        period, _, ranking_type = key.partition("_")
        for rank, trader in enumerate(traders, 1):
            trader_id = trader.get("traderId")
            if trader_id not in profiles:
                continue
            details = (profiles[trader_id].get("sourceDetails") or {})
            rows.append({
                "trader_id": trader_id,
                "period": period,
                "ranking_type": ranking_type,
                "rank_position": rank,
                "roi": trader.get("roi"),
                "pnl": trader.get("pnl"),
                "win_rate": details.get("winRate"),
                "max_drawdown": details.get("maxDrawdown"),
                "days_active": details.get("daysActive"),
                "subscribers": details.get("subscribers"),
                "net_transfer": details.get("netTransfer"),
                "snapshot_at": manifest().get("extracted_at"),
            })
    return rows


def build_profile_daily() -> list[dict]:
    """Seed: raw_trader_profile_daily. Grain: one row per trader x snapshot date."""
    rows = []
    snap_dir = SOURCE_PATH / "_profile_snapshots"
    if not snap_dir.exists():
        return rows
    for path in sorted(snap_dir.glob("*.json")):
        trader_id, _, snapshot_date = path.stem.rpartition("_")
        profile = load_json(path)
        details = profile.get("sourceDetails", {}) or {}
        rows.append({
            "trader_id": trader_id,
            "snapshot_date": snapshot_date,
            "subscribers": details.get("subscribers"),
            "win_rate": details.get("winRate"),
            "max_drawdown": details.get("maxDrawdown"),
            "days_active": details.get("daysActive"),
            "margin_balance": details.get("umMarginBalance"),
            "is_ai": details.get("isAi"),
        })
    return rows


def build_manifest() -> list[dict]:
    """Seed: raw_extract_manifest. Grain: one row per extraction run."""
    meta = manifest()
    if not meta:
        return []
    positions = meta.get("positions_extracted") or 0
    orders = meta.get("orders_extracted") or 0
    return [{
        "run_id": meta.get("run_id"),
        "source": meta.get("source"),
        "symbols": ",".join(meta.get("symbols", [])),
        "start_date": meta.get("date_from"),
        "end_date": meta.get("date_to"),
        "positions_extracted": positions,
        "orders_extracted": orders,
        "rows_extracted": positions + orders,
        "extracted_at": meta.get("extracted_at"),
    }]


# =============================================================================
# MAIN
# =============================================================================

SEEDS = [
    ("raw_traders.csv", [
        "trader_id", "trader_name", "is_ai", "avatar_url",
        "account_name", "subscribers_at_extract", "extracted_at"], build_traders),
    ("raw_symbols.csv", [
        "symbol", "base_asset", "quote_asset", "market_type",
        "contract_type"], build_symbols),
    ("raw_closed_positions.csv", [
        "position_id", "trader_id", "symbol", "market_type", "side",
        "margin_mode", "leverage", "opened_at_utc", "closed_at_utc",
        "entry_price", "close_price", "opened_volume", "closed_volume",
        "gross_pnl", "trading_fee", "funding_fee", "liquidation_fee",
        "net_pnl", "pnl_currency", "status", "extracted_at"], build_closed_positions),
    ("raw_orders.csv", [
        "order_id", "position_id", "trader_id", "symbol", "side",
        "position_side", "action", "order_type", "status", "price",
        "quantity", "quote_quantity", "leverage", "realized_pnl",
        "trading_fee", "created_at_utc", "updated_at_utc"], build_orders),
    ("raw_trader_performance_actual.csv", [
        "trader_id", "period", "ranking_type", "rank_position", "roi", "pnl",
        "win_rate", "max_drawdown", "days_active", "subscribers",
        "net_transfer", "snapshot_at"], build_performance_actual),
    ("raw_trader_profile_daily.csv", [
        "trader_id", "snapshot_date", "subscribers", "win_rate",
        "max_drawdown", "days_active", "margin_balance", "is_ai"],
        build_profile_daily),
    ("raw_extract_manifest.csv", [
        "run_id", "source", "symbols", "start_date", "end_date",
        "positions_extracted", "orders_extracted", "rows_extracted",
        "extracted_at"], build_manifest),
]


def main() -> int:
    if not SOURCE_PATH.exists():
        print(f"No raw data: {SOURCE_PATH} - run extract_traders.py first.")
        return 1

    SEEDS_PATH.mkdir(parents=True, exist_ok=True)
    print("=" * 64)
    print("Building dbt seeds from raw trader JSON")
    print("=" * 64)
    print(f"  source: {SOURCE_PATH}")
    print(f"  target: {SEEDS_PATH}")

    for filename, columns, builder in SEEDS:
        write_csv(filename, columns, builder())

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())