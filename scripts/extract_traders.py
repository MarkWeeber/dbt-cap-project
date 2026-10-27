#!/usr/bin/env python3
"""
Binance Smart Money trader extractor -> per-trader JSON under data/raw/traders.

Stage 1 of 2 of the trader-seed pipeline:
    extract_traders.py  ->  data/raw/traders/     (raw JSON, gitignored)
    build_seeds.py      ->  dbt_project/seeds     (CSV seeds for dbt)

The date window (DATE_FROM .. DATE_TO) is the only bound on trade volume:
everything inside it is pulled. Idempotent: an existing trader folder is
skipped, so a re-run only fills the gaps. Profile snapshots are dated and
accumulate, so the SCD2 seed can grow across daily runs.
"""

import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, time as dtime, timezone
from pathlib import Path

import requests

# =============================================================================
# CONFIGURATION - modify these values
# =============================================================================

# Prefer the environment variable:  export KOPYON_API_KEY="kpy_..."
KOPYON_API_KEY = os.getenv("KOPYON_API_KEY", "kpy_ARSpZqyTa6Xk7Iy0W2sYN5xINZ-lz_jN")

# Anchored to the script location, so the CWD does not matter.
SAVE_PATH = Path(__file__).resolve().parent.parent / "data" / "raw" / "traders"

# --- Trader selection --------------------------------------------------------
NUM_TRADERS = 10            # how many traders to keep
MAX_CANDIDATES = 60         # how deep into the ranking to look for Latin names
PRIMARY_PERIOD = "30D"      # ranking used to pick traders
PRIMARY_TYPE = "PNL"        # PNL or ROI

# --- Trade window (UTC) ------------------------------------------------------
# This window is the ONLY bound on volume. Align it with a ranking period if
# you want the reconciliation seed to be comparable.
DATE_FROM = "2026-08-01"
DATE_TO = "2026-09-30"

# --- Trade filters -----------------------------------------------------------
# Applies server-side to positions and client-side to orders (the orderHistory
# route has no `symbol` parameter). Empty list = every symbol.
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT"]
MARKET_TYPES = ["UM"]              # UM = USDⓈ-M futures, CM = COIN-M futures

# --- Rankings persisted for the reconciliation seed --------------------------
RANKING_PERIODS = ["30D", "90D", "ALL"]
RANKING_TYPES = ["PNL", "ROI"]

# --- HTTP behaviour ----------------------------------------------------------
REQUEST_DELAY = 0.5         # seconds between calls (free tier: 120/min)
MAX_RETRIES = 3
TIMEOUT = 30
ROWS_PER_PAGE = 20          # API maximum per page
MAX_PAGES_PER_TRADER = 50   # safety valve against endless pagination

BASE_URL = "https://api.kopyon.com/v5/binance-smart-money"
HEADERS = {"X-API-KEY": KOPYON_API_KEY}

# Names must be pure Latin (letters/digits/space/._-) with >=1 Latin letter.
LATIN_NAME_RE = re.compile(r"^[A-Za-z0-9 ._\-]+$")


# =============================================================================
# HELPERS
# =============================================================================

def is_latin_name(name: str) -> bool:
    """Return True if a trader name is safe to keep in an English project."""
    if not name:
        return False
    name = name.strip()
    if not LATIN_NAME_RE.match(name):
        return False
    return any(ch.isalpha() and ch.isascii() for ch in name)


def safe_name(name: str) -> str:
    """Turn a trader name into an ASCII-safe folder fragment."""
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")[:30] or "trader"


def to_epoch_ms(date_str: str, end_of_day: bool = False) -> int:
    """Convert 'YYYY-MM-DD' to a UTC Unix timestamp in milliseconds."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if end_of_day:
        dt = datetime.combine(dt.date(), dtime(23, 59, 59))
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def api_get(path: str, params: dict) -> dict | None:
    """GET against the Kopyon API with retries and rate-limit handling."""
    url = f"{BASE_URL}/{path}"
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)

            if resp.status_code == 401:
                print("  [auth] invalid API key - aborting")
                return None
            if resp.status_code == 429:
                print("  [rate limit] waiting 60s")
                time.sleep(60)
                continue
            if resp.status_code == 422:
                print(f"  [422] invalid request: {resp.text[:180]}")
                return None

            resp.raise_for_status()
            return resp.json().get("data")

        except requests.exceptions.RequestException as exc:
            print(f"  [retry {attempt + 1}/{MAX_RETRIES}] {exc}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
    return None


def write_json(path: Path, payload) -> None:
    """Write pretty JSON, creating parent folders as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)


# =============================================================================
# TRADER SELECTION
# =============================================================================

def fetch_ranking(period: str, ranking_type: str, limit: int = MAX_CANDIDATES) -> list[dict]:
    """Fetch one full ranking (paginated) and return up to `limit` traders."""
    collected, page = [], 1
    while len(collected) < limit:
        data = api_get("traders", {
            "rankingType": ranking_type,
            "order": "DESC",
            "onlyShowSharingPosition": "true",
            "period": period,
            "page": page,
            "rows": ROWS_PER_PAGE,
        })
        if not data:
            break

        traders = data.get("traders", [])
        collected.extend(traders)
        if not data.get("hasMore") or not traders:
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    return collected[:limit]


# =============================================================================
# TRADER DATA
# =============================================================================

def get_profile(trader_id: str) -> dict | None:
    """Fetch a trader's statistics profile."""
    return api_get("traderProfile", {"traderId": trader_id})


def get_open_positions(trader_id: str, market_type: str) -> dict:
    """Fetch currently open positions for one market."""
    data = api_get("openPositions", {"traderId": trader_id, "marketType": market_type})
    return data or {"positions": [], "hidden": None}


def get_position_history(trader_id: str, market_type: str) -> dict:
    """
    Fetch ALL closed positions in the window for one trader / one market.

    Paginates per symbol with the API cursor until it is exhausted. The date
    window and SYMBOLS filter are applied server-side, so no row cap is used.
    """
    collected = []
    for symbol in (SYMBOLS or [None]):
        cursor, pages = None, 0
        while pages < MAX_PAGES_PER_TRADER:
            params = {
                "traderId": trader_id,
                "marketType": market_type,
                "rows": ROWS_PER_PAGE,
                "startTime": to_epoch_ms(DATE_FROM),
                "endTime": to_epoch_ms(DATE_TO, end_of_day=True),
            }
            if symbol:
                params["symbol"] = symbol
            if cursor:
                params["cursor"] = cursor

            data = api_get("positionHistory", params)
            if not data:
                break

            collected.extend(data.get("positions", []))
            cursor = data.get("nextCursor")
            pages += 1
            if not cursor:
                break
            time.sleep(REQUEST_DELAY)

    return {"positions": collected}


def get_order_history(trader_id: str, market_type: str) -> dict:
    """
    Fetch ALL executed orders in the window for one trader / one market.

    The orderHistory route has NO `symbol` parameter, so the SYMBOLS filter is
    applied client-side after pagination.
    """
    collected, cursor, pages = [], None, 0
    while pages < MAX_PAGES_PER_TRADER:
        params = {
            "traderId": trader_id,
            "marketType": market_type,
            "rows": ROWS_PER_PAGE,
            "startTime": to_epoch_ms(DATE_FROM),
            "endTime": to_epoch_ms(DATE_TO, end_of_day=True),
        }
        if cursor:
            params["cursor"] = cursor

        data = api_get("orderHistory", params)
        if not data:
            break

        collected.extend(data.get("orders", []))
        cursor = data.get("nextCursor")
        pages += 1
        if not cursor:
            break
        time.sleep(REQUEST_DELAY)

    if SYMBOLS:
        collected = [o for o in collected if o.get("symbol") in SYMBOLS]

    return {"orders": collected}


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    print("=" * 64)
    print("Binance Smart Money - trader extractor")
    print("=" * 64)
    print(f"  output      : {SAVE_PATH}")
    print(f"  traders     : {NUM_TRADERS}")
    print(f"  ranking     : {PRIMARY_PERIOD} {PRIMARY_TYPE}")
    print(f"  window      : {DATE_FROM} -> {DATE_TO}")
    print(f"  symbols     : {SYMBOLS or 'ALL'}")
    print(f"  markets     : {MARKET_TYPES}")
    print("=" * 64)

    if KOPYON_API_KEY.startswith("YOUR"):
        print("Set KOPYON_API_KEY first.")
        return 1

    SAVE_PATH.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())[:8]
    extracted_at = datetime.now(timezone.utc).isoformat()
    snapshot_date = extracted_at[:10]

    print("Fetching rankings...")
    rankings = {}
    for period in RANKING_PERIODS:
        for ranking_type in RANKING_TYPES:
            key = f"{period}_{ranking_type}"
            rankings[key] = fetch_ranking(period, ranking_type)
            print(f"  {key}: {len(rankings[key])} traders")
            time.sleep(REQUEST_DELAY)
    write_json(SAVE_PATH / "_ranking.json", rankings)

    candidates = [t for t in rankings.get(f"{PRIMARY_PERIOD}_{PRIMARY_TYPE}", [])
                  if is_latin_name(t.get("name", ""))]
    print(f"\nLatin-named candidates: {len(candidates)}")

    saved, total_positions, total_orders, attempts = 0, 0, 0, 0
    for candidate in candidates:
        if saved >= NUM_TRADERS:
            break
        
        attempts += 1
        
        trader_id = candidate.get("traderId")
        name = candidate.get("name", "unknown")
        folder = SAVE_PATH / f"{safe_name(name)}_{trader_id}"

        if folder.exists():
            print(f"[{saved + 1}/{NUM_TRADERS}] {name} - exists, skip")
            saved += 1
            continue

        #print(f"[{saved + 1}/{NUM_TRADERS}] {name} ({trader_id})")
        print(f"[{attempts}] {name} ({trader_id})")

        profile = get_profile(trader_id)
        time.sleep(REQUEST_DELAY)

        positions = {mt: get_position_history(trader_id, mt) for mt in MARKET_TYPES}
        for _ in MARKET_TYPES:
            time.sleep(REQUEST_DELAY)
        orders = {mt: get_order_history(trader_id, mt) for mt in MARKET_TYPES}
        time.sleep(REQUEST_DELAY)

        n_pos = sum(len(v.get("positions", [])) for v in positions.values())
        if n_pos == 0:
            print("  no trades in window, skipping")
            continue

        n_ord = sum(len(v.get("orders", [])) for v in orders.values())

        write_json(folder / "profile.json", profile or {})
        for mt in MARKET_TYPES:
            write_json(folder / f"positions_{mt}.json", positions[mt])
            write_json(folder / f"orders_{mt}.json", orders[mt])

        if profile:
            write_json(SAVE_PATH / "_profile_snapshots" / f"{trader_id}_{snapshot_date}.json", profile)

        saved += 1
        total_positions += n_pos
        total_orders += n_ord
        print(f"  -> {n_pos} positions, {n_ord} orders")

    write_json(SAVE_PATH / "_manifest.json", {
        "run_id": run_id,
        "source": "kopyon_api",
        "symbols": SYMBOLS,
        "market_types": MARKET_TYPES,
        "date_from": DATE_FROM,
        "date_to": DATE_TO,
        "traders_saved": saved,
        "positions_extracted": total_positions,
        "orders_extracted": total_orders,
        "extracted_at": extracted_at,
    })

    print(f"\nDone. traders={saved}, positions={total_positions}, orders={total_orders}")
    return 0


if __name__ == "__main__":
    sys.exit(main())