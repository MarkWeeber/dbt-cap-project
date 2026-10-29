#!/usr/bin/env python3
"""
Trader JSON (data/raw/traders) -> CSV seeds (dbt_project/seeds).

Wipes every existing *.csv in the seeds directory first, so a seed that is no
longer produced cannot linger. schema.yml is left alone.

Seeds stay deliberately raw: no surrogate keys and no order->position link.
Both belong to dbt (dbt_utils.generate_surrogate_key in staging, a range join
in the intermediate layer). Python only deduplicates and reports a match-rate
estimate for QA.

Usage:
    python3 build_seeds.py
"""

import csv
import json
import sys
from bisect import bisect_left
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import MARKET_TYPE, SYMBOLS, TRADERS_DIR   # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
SEEDS_DIR = REPO_ROOT / "dbt_project" / "seeds"

# How far an order timestamp may sit from a position boundary and still count
# as the same event. Observed jitter is a few milliseconds.
POSITION_MATCH_SLACK_MS = 5_000

QUOTE_ASSETS = ("USDT", "USDC", "BUSD", "FDUSD", "TUSD", "BTC", "ETH", "BNB")

POSITION_FIELDS = [
    "trader_id", "position_id", "market_type", "symbol", "symbol_is_approved",
    "side", "status", "is_fully_closed", "margin_mode", "leverage",
    "entry_price", "close_price", "opened_volume", "closed_volume",
    "remaining_volume", "max_open_interest", "gross_pnl", "funding_pnl",
    "trading_fee", "liquidation_fee", "net_pnl", "roi", "pnl_currency",
    "opened_at", "closed_at", "updated_at", "opened_at_ts", "closed_at_ts",
]

ORDER_FIELDS = [
    "trader_id", "market_type", "symbol", "symbol_is_approved", "side",
    "position_side", "price", "quantity", "quote_quantity", "created_at",
    "created_at_ts", "updated_at",
]

OPEN_POSITION_FIELDS = [
    "trader_id", "market_type", "symbol", "symbol_is_approved", "side", "size",
    "entry_price", "mark_price", "leverage", "margin_mode", "margin",
    "unrealized_pnl", "roi", "maintenance_margin", "margin_ratio", "pnl_currency",
]

TRADER_FIELDS = [
    "trader_id", "name", "bio", "avatar_url", "account_name", "is_ai",
    "enabled", "sharing_positions", "days_active", "subscribers", "win_rate",
    "max_drawdown", "lifetime_pnl", "um_margin_balance", "cm_margin_balance",
    "net_transfer", "profile_roi",
]

MANIFEST_FIELDS = [
    "trader_id", "positions", "orders", "open_positions", "is_partial",
]

APPROVED = set(SYMBOLS)


# --------------------------------------------------------------------- helpers

def read_json(path: Path, key: str | None = None) -> list:
    """Read a JSON file, returning [] when missing. Optionally unwrap one key."""
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"  WARN unreadable {path.name}: {exc}")
        return []
    if key is None:
        return payload if isinstance(payload, list) else [payload]
    return (payload or {}).get(key) or []


def split_symbol(symbol: str) -> tuple[str, str]:
    for quote in QUOTE_ASSETS:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)], quote
    return symbol, ""


def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return ""


def write_csv(name: str, fields: list[str], rows: list[dict]) -> Path:
    path = SEEDS_DIR / name
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n",
                                extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def iter_traders():
    """Yield (trader_id, folder) for every trader directory."""
    if not TRADERS_DIR.is_dir():
        return
    for folder in sorted(TRADERS_DIR.iterdir()):
        if not folder.is_dir():
            continue
        _, _, trader_id = folder.name.rpartition("_")
        if trader_id.isdigit():
            yield trader_id, folder


# ------------------------------------------------------------------ extraction

def build_symbols() -> list[dict]:
    rows = []
    for symbol in SYMBOLS:
        base, quote = split_symbol(symbol)
        rows.append({
            "symbol": symbol,
            "base_asset": base,
            "quote_asset": quote,
            "is_approved": "true",
        })
    return rows


def build_traders() -> list[dict]:
    rows = []
    for trader_id, folder in iter_traders():
        profile = read_json(folder / "profile.json")
        profile = profile[0] if profile else {}
        source = profile.get("sourceDetails") or {}
        rows.append({
            "trader_id": trader_id,
            "name": profile.get("name", ""),
            "bio": profile.get("bio", ""),
            "avatar_url": profile.get("avatarUrl", ""),
            "account_name": source.get("accountName", ""),
            "is_ai": source.get("isAi", ""),
            "enabled": source.get("enabled", ""),
            "sharing_positions": source.get("sharingPositions", ""),
            "days_active": source.get("daysActive", ""),
            "subscribers": source.get("subscribers", ""),
            "win_rate": source.get("winRate", ""),
            "max_drawdown": source.get("maxDrawdown", ""),
            "lifetime_pnl": source.get("pnl", ""),
            "um_margin_balance": source.get("umMarginBalance", ""),
            "cm_margin_balance": source.get("cmMarginBalance", ""),
            "net_transfer": source.get("netTransfer", ""),
            "profile_roi": profile.get("roi", ""),
        })
    return rows


def build_positions() -> tuple[list[dict], dict[str, dict]]:
    """Returns position rows plus a per-trader (symbol, side) time index for QA."""
    rows, index = [], defaultdict(dict)

    for trader_id, folder in iter_traders():
        seen = set()
        per_trader = defaultdict(list)

        for position in read_json(folder / f"positions_{MARKET_TYPE}.json", "positions"):
            position_id = position.get("positionId")
            if not position_id:
                continue

            side = position.get("side") or ""
            signature = (position_id, side, position.get("openedAt"))
            if signature in seen:
                continue
            seen.add(signature)

            opened = position.get("openedVolume") or 0
            closed = position.get("closedVolume") or 0
            symbol = position.get("symbol") or ""
            status = position.get("status") or ""

            rows.append({
                "trader_id": trader_id,
                "position_id": position_id,
                "market_type": (position.get("sourceDetails") or {}).get("marketType", MARKET_TYPE),
                "symbol": symbol,
                "symbol_is_approved": "true" if symbol in APPROVED else "false",
                "side": side,
                "status": status,
                "is_fully_closed": "true" if status == "CLOSED" else "false",
                "margin_mode": position.get("marginMode", ""),
                "leverage": position.get("leverage", ""),
                "entry_price": position.get("entryPrice", ""),
                "close_price": position.get("closePrice", ""),
                "opened_volume": opened,
                "closed_volume": closed,
                "remaining_volume": round(float(opened) - float(closed), 10),
                "max_open_interest": position.get("maxOpenInterest", ""),
                "gross_pnl": position.get("grossPnl", ""),
                "funding_pnl": position.get("fundingPnl", ""),
                "trading_fee": position.get("tradingFee", ""),
                "liquidation_fee": position.get("liquidationFee", ""),
                "net_pnl": position.get("netPnl", ""),
                "roi": position.get("roi", ""),
                "pnl_currency": position.get("pnlCurrency", ""),
                "opened_at": position.get("openedAt", ""),
                "closed_at": position.get("closedAt", ""),
                "updated_at": position.get("updatedAt", ""),
                "opened_at_ts": to_int(position.get("openedAt")),
                "closed_at_ts": to_int(position.get("closedAt")),
            })

            # Key on (symbol, order side) only: positionSide arrives as BOTH for
            # one-way accounts, so it cannot be part of the key.
            open_side = "BUY" if side == "LONG" else "SELL"
            close_side = "SELL" if side == "LONG" else "BUY"
            opened_ts = to_int(position.get("openedAt"))
            closed_ts = to_int(position.get("closedAt"))
            span = (opened_ts, closed_ts, position_id)
            if opened_ts != "":
                per_trader[(symbol, open_side)].append(span)
            if closed_ts != "":
                per_trader[(symbol, close_side)].append(span)

        for key, stamps in per_trader.items():
            index[trader_id][key] = sorted(stamps)

    return rows, index


def estimate_match(order: dict, lookup: dict) -> bool:
    """QA only: does this order fall inside a position's lifetime?"""
    entries = lookup.get((order.get("symbol"), order.get("side")))
    if not entries:
        return False
    created = to_int(order.get("createdAt"))
    if created == "":
        return False

    for opened_ts, closed_ts, _ in entries:
        start = opened_ts - POSITION_MATCH_SLACK_MS
        end = (closed_ts if closed_ts != "" else opened_ts) + POSITION_MATCH_SLACK_MS
        if start <= created <= end:
            return True
    return False


def build_orders(index: dict[str, dict]) -> tuple[list[dict], int, int]:
    rows, matched = [], 0

    for trader_id, folder in iter_traders():
        lookup = index.get(trader_id, {})
        seen = set()

        for order in read_json(folder / f"orders_{MARKET_TYPE}.json", "orders"):
            symbol = order.get("symbol") or ""
            side = order.get("side") or ""
            created = to_int(order.get("createdAt"))

            # Broker duplicates differ only in timestamp precision, so the
            # second-truncated value is the dedupe key.
            signature = (symbol, side, order.get("positionSide"),
                         order.get("price"), order.get("quantity"),
                         int(created) // 1000 if created != "" else "")
            if signature in seen:
                continue
            seen.add(signature)

            if estimate_match(order, lookup):
                matched += 1

            rows.append({
                "trader_id": trader_id,
                "market_type": (order.get("sourceDetails") or {}).get("marketType", MARKET_TYPE),
                "symbol": symbol,
                "symbol_is_approved": "true" if symbol in APPROVED else "false",
                "side": side,
                "position_side": order.get("positionSide") or "",
                "price": order.get("price", ""),
                "quantity": order.get("quantity", ""),
                "quote_quantity": order.get("quoteQuantity", ""),
                "created_at": order.get("createdAt", ""),
                "created_at_ts": created,
                "updated_at": order.get("updatedAt", ""),
            })

    return rows, matched, len(rows)


def build_open_positions() -> list[dict]:
    rows = []
    for trader_id, folder in iter_traders():
        seen = set()
        for entry in read_json(folder / f"open_positions_{MARKET_TYPE}.json", "positions"):
            symbol = entry.get("symbol") or ""
            signature = (symbol, entry.get("side"), entry.get("entryPrice"))
            if signature in seen:
                continue
            seen.add(signature)
            source = entry.get("sourceDetails") or {}

            rows.append({
                "trader_id": trader_id,
                "market_type": source.get("marketType", MARKET_TYPE),
                "symbol": symbol,
                "symbol_is_approved": "true" if symbol in APPROVED else "false",
                "side": entry.get("side", ""),
                "size": entry.get("size", ""),
                "entry_price": entry.get("entryPrice", ""),
                "mark_price": entry.get("markPrice", ""),
                "leverage": entry.get("leverage", ""),
                "margin_mode": entry.get("marginMode", ""),
                "margin": entry.get("margin", ""),
                "unrealized_pnl": entry.get("unrealizedPnl", ""),
                "roi": entry.get("roi", ""),
                "maintenance_margin": source.get("maintenanceMargin", ""),
                "margin_ratio": source.get("marginRatio", ""),
                "pnl_currency": entry.get("pnlCurrency", ""),
            })
    return rows


def build_manifest() -> list[dict]:
    rows = []
    for trader_id, folder in iter_traders():
        rows.append({
            "trader_id": trader_id,
            "positions": len(read_json(folder / f"positions_{MARKET_TYPE}.json", "positions")),
            "orders": len(read_json(folder / f"orders_{MARKET_TYPE}.json", "orders")),
            "open_positions": len(read_json(folder / f"open_positions_{MARKET_TYPE}.json", "positions")),
            "is_partial": "true" if (folder / "_INCOMPLETE").exists() else "false",
        })
    return rows


# ------------------------------------------------------------------------ main

def main() -> int:
    if not TRADERS_DIR.is_dir():
        print(f"missing {TRADERS_DIR} - run extract_traders.py first")
        return 1

    SEEDS_DIR.mkdir(parents=True, exist_ok=True)
    removed = 0
    for stale in SEEDS_DIR.glob("*.csv"):
        stale.unlink()
        removed += 1
    print(f"cleared {removed} stale csv file(s) from {SEEDS_DIR}")

    traders = build_traders()
    if not traders:
        print("no trader folders found")
        return 1

    positions, index = build_positions()
    orders, matched, total_orders = build_orders(index)
    open_positions = build_open_positions()

    written = [
        write_csv("symbols.csv",
                  ["symbol", "base_asset", "quote_asset", "is_approved"], build_symbols()),
        write_csv("traders.csv", TRADER_FIELDS, traders),
        write_csv("positions.csv", POSITION_FIELDS, positions),
        write_csv("orders.csv", ORDER_FIELDS, orders),
        write_csv("open_positions.csv", OPEN_POSITION_FIELDS, open_positions),
        write_csv("extraction_manifest.csv", MANIFEST_FIELDS, build_manifest()),
    ]

    approved = sum(1 for p in positions if p["symbol_is_approved"] == "true")
    share = approved / len(positions) * 100 if positions else 0
    order_rate = matched / total_orders * 100 if total_orders else 0

    print(f"\ntraders   : {len(traders)}")
    print(f"positions : {len(positions)}  (approved symbols: {approved} = {share:.1f}%)")
    print(f"orders    : {len(orders)}  (QA match estimate: {matched} = {order_rate:.1f}%)")
    print(f"open pos  : {len(open_positions)}")
    print("\nwritten:")
    for path in written:
        print(f"  {path.relative_to(REPO_ROOT)}  {path.stat().st_size} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())