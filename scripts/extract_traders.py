#!/usr/bin/env python3
"""
Binance Smart Money trader extractor -> per-trader JSON under data/raw/traders.

Stage 1 of 2 of the trader-seed pipeline:
    extract_traders.py  ->  data/raw/traders/     (raw JSON, gitignored)
    build_seeds.py      ->  dbt_project/seeds     (CSV seeds for dbt)

The API caps a single history query, so the window is sliced into CHUNK_DAYS
slices and a slice that still hits the cap is split again. A slice whose
request dies is reported as incomplete rather than silently treated as empty.

Usage:
    python3 extract_traders.py                 # full run
    python3 extract_traders.py --limit 3       # smoke test
    python3 extract_traders.py --force         # ignore existing folders
"""

import argparse
import json
import os
import re
import sys
import time
import uuid
import shutil
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (                          # noqa: E402
    API_BASE, API_KEY_ENV, DATE_FROM, DATE_TO, MARKET_TYPE,
    MIN_POSITIONS, N_TRADERS, RANKING_PERIOD,
    REQUEST_DELAY, TIMEOUT, TRADERS_DIR, date_chunks, to_ms,
)

SOURCE = "binance-smart-money"
URL = f"{API_BASE}/v5/{SOURCE}"
HEADERS = {"accept": "application/json", "X-API-KEY": ""}

ROWS_PER_PAGE = 20              # API maximum
PAGE_SIZE_LADDER = (ROWS_PER_PAGE, 10, 5, 2, 1)
API_ROW_CAP = 501               # observed cap per history query
MAX_PAGES = 60                  # pagination safety valve
MAX_RETRIES = 6                 # 5xx / timeouts get this many attempts
MAX_BACKOFF = 30                # seconds, cap on exponential backoff
DAY_MS = 86_400_000
MIN_SLICE = timedelta(days=1)   # never split a slice narrower than this
RANKING_LIMIT = 200             # candidates per ranking period
RANKING_PERIODS = (RANKING_PERIOD, "7D", "30D")
INCOMPLETE_MARKER = "_INCOMPLETE"

LATIN_NAME_RE = re.compile(r"^[A-Za-z0-9 ._\-]+$")


# --------------------------------------------------------------------- helpers

def as_datetime(value) -> datetime:
    """config.date_chunks() yields datetime.date; normalise to datetime."""
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, dt_time.min)


def is_latin_name(name: str) -> bool:
    """Keep only ASCII-safe trader names so the repo stays readable."""
    if not name or not LATIN_NAME_RE.match(name.strip()):
        return False
    return any(c.isalpha() and c.isascii() for c in name)


def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")[:30] or "trader"


def api_get(path: str, params: dict, retries: int = MAX_RETRIES) -> dict | None:
    """GET with retries and 429 handling. Returns the `data` object, or None on failure."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(f"{URL}/{path}", headers=HEADERS, params=params, timeout=TIMEOUT)

            if r.status_code == 401:
                print("  [auth] invalid API key - aborting")
                return None
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 60))
                print(f"  [rate limit] waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 422:
                print(f"  [422] {r.text[:200]}")
                return None

            r.raise_for_status()
            return r.json().get("data")

        except requests.exceptions.RequestException as exc:
            print(f"  [retry {attempt}/{retries}] {exc}")
            if attempt < retries:
                time.sleep(min(2 ** attempt, MAX_BACKOFF))
    return None


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)


def window_ms(start, end):
    """Inclusive [start, end] date range -> (start_ms, end_ms)."""
    return to_ms(as_datetime(start)), to_ms(as_datetime(end)) + DAY_MS - 1


def trader_folder_complete(folder: Path) -> bool:
    """True only when every expected file exists and no retry marker is left."""
    if (folder / INCOMPLETE_MARKER).exists():
        return False
    needed = (
        "profile.json",
        f"positions_{MARKET_TYPE}.json",
        f"orders_{MARKET_TYPE}.json",
        f"open_positions_{MARKET_TYPE}.json",
    )
    return all((folder / n).is_file() and (folder / n).stat().st_size > 0 for n in needed)


# ---------------------------------------------------------------- pagination

def paged(path: str, trader_id: str, start, end, key: str,
          rows_per_page: int = ROWS_PER_PAGE, retries: int = 1) -> tuple[list[dict], bool]:
    """Cursor-paginate one history slice from the very beginning.

    The cursor is bound to the exact query (including `rows`), so a page-size
    change cannot resume an existing chain - it has to restart the window.
    """
    rows, cursor, pages = [], None, 0
    s_ms, e_ms = window_ms(start, end)

    while pages < MAX_PAGES:
        params = {
            "traderId": trader_id,
            "marketType": MARKET_TYPE,
            "rows": rows_per_page,
            "startTime": s_ms,
            "endTime": e_ms,
        }
        if cursor:
            params["cursor"] = cursor

        data = api_get(path, params, retries=retries)
        if data is None:
            return rows, False

        rows.extend(data.get(key) or [])
        cursor = data.get("nextCursor")
        pages += 1
        if not cursor:
            return rows, True
        time.sleep(REQUEST_DELAY)

    return rows, True


def sliced(path: str, trader_id: str, start, end, key: str, depth: int = 0) -> tuple[list[dict], bool]:
    """Fetch a slice: shrink the page first, split the window last.

    Failures upstream are deterministic, so retrying the same request rarely
    helps - we escalate immediately and keep whichever rung yielded most rows.
    Splitting is what recovers everything *after* an unservable record, because
    a window starting past it gets a clean cursor chain.
    """
    start = as_datetime(start)
    end = as_datetime(end)
    splittable = (end - start) > MIN_SLICE
    last = len(PAGE_SIZE_LADDER) - 1

    rows, complete = [], False
    for idx, rows_per_page in enumerate(PAGE_SIZE_LADDER):
        attempt, ok = paged(path, trader_id, start, end, key,
                            rows_per_page=rows_per_page,
                            retries=2 if idx == last else 1)
        if ok:
            rows, complete = attempt, True
            break
        if len(attempt) > len(rows):
            rows = attempt
        if idx + 1 < len(PAGE_SIZE_LADDER):
            print(f"  [pagesize] {path} {start:%Y-%m-%d}..{end:%Y-%m-%d} "
                  f"rows={rows_per_page} failed, best {len(rows)} "
                  f"-> {PAGE_SIZE_LADDER[idx + 1]}")

    if complete and len(rows) < API_ROW_CAP:
        return rows, True

    if not splittable:
        if len(rows) >= API_ROW_CAP:
            print(f"  [cap] {path} {start:%Y-%m-%d}..{end:%Y-%m-%d} "
                  f"{len(rows)} rows at floor - accepting")
        if not complete:
            print(f"  [INCOMPLETE] {path} {start:%Y-%m-%d}..{end:%Y-%m-%d} kept={len(rows)}")
        return rows, complete

    raw_mid = start + (end - start) / 2
    mid = raw_mid.replace(hour=0, minute=0, second=0, microsecond=0)
    if not (start < mid < end):
        mid = raw_mid

    reason = "cap" if complete else "err"
    pad = "  " * (depth + 1)
    print(f"{pad}[SPLIT-{reason}] {path} {start:%Y-%m-%d}..{end:%Y-%m-%d} rows={len(rows)}")

    left, ok_l = sliced(path, trader_id, start, mid, key, depth + 1)
    right, ok_r = sliced(path, trader_id, mid, end, key, depth + 1)
    return left + right, ok_l and ok_r


def fetch_history(path: str, trader_id: str, key: str, dedup_keys: tuple[str, ...]) -> tuple[dict, bool]:
    """Walk every window chunk and return (deduplicated rows, complete).

    The halves of a split share their boundary day, so dedup is what keeps the
    result clean - the overlap is intentional and leaves no gaps.
    """
    seen, rows, complete = set(), [], True
    for start, end in date_chunks():
        chunk, chunk_ok = sliced(path, trader_id, start, end, key)
        complete = complete and chunk_ok
        for row in chunk:
            sig = tuple(str(row.get(k)) for k in dedup_keys)
            if sig in seen:
                continue
            seen.add(sig)
            rows.append(row)
        time.sleep(REQUEST_DELAY)
    return {key: rows}, complete


# ------------------------------------------------------------------ selection

def fetch_ranking(period: str, limit: int = RANKING_LIMIT) -> list[dict]:
    collected, page = [], 1
    while len(collected) < limit:
        data = api_get("traders", {
            "rankingType": "PNL", "order": "DESC",
            "onlyShowSharingPosition": "true",
            "period": period, "page": page, "rows": ROWS_PER_PAGE,
        })
        if not data:
            break
        traders = data.get("traders") or []
        collected.extend(traders)
        if not data.get("hasMore") or not traders:
            break
        page += 1
        time.sleep(REQUEST_DELAY)
    return collected[:limit]


# ------------------------------------------------------------------------ main

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract Binance Smart Money trader history.")
    p.add_argument("--limit", type=int, default=N_TRADERS,
                   help="stop after this many accepted traders")
    p.add_argument("--force", action="store_true",
                   help="re-extract even if the trader folder already exists")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    limit = args.limit

    print("=" * 64)
    print("Binance Smart Money - trader extractor")
    print("=" * 64)
    print(f"  output   : {TRADERS_DIR}")
    print(f"  traders  : {limit} (min {MIN_POSITIONS} positions)")
    print(f"  window   : {DATE_FROM} -> {DATE_TO}")
    print(f"  force    : {args.force}")
    print("=" * 64)

    key = os.getenv(API_KEY_ENV)
    if not key:
        print(f"Set {API_KEY_ENV} first.")
        return 1
    HEADERS["X-API-KEY"] = key

    TRADERS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())[:8]
    extracted_at = datetime.now(timezone.utc).isoformat()

    print("Fetching ranking...")
    ranking, candidates, seen_ids = {}, [], set()
    for period in RANKING_PERIODS:
        rows = fetch_ranking(period)
        ranking[period] = rows
        for t in rows:
            tid = t.get("traderId")
            if tid and tid not in seen_ids:
                seen_ids.add(tid)
                candidates.append(t)
        print(f"  {period}: {len(rows)} traders")

    candidates = [t for t in candidates if is_latin_name(t.get("name", ""))]
    write_json(TRADERS_DIR / "_ranking.json",
               {f"{p}_PNL": rows for p, rows in ranking.items()})
    print(f"  {len(seen_ids)} unique, {len(candidates)} with Latin names\n")

    saved = positions_total = 0
    skipped = 0
    examined = 0
    incomplete_traders = []

    for candidate in candidates:
        if saved >= limit:
            break
        examined += 1

        trader_id = candidate["traderId"]
        name = candidate.get("name", "unknown")
        folder = TRADERS_DIR / f"{safe_name(name)}_{trader_id}"

        # Remember how much the previous attempt managed to salvage.
        marker = folder / INCOMPLETE_MARKER
        prev_kept = None
        if marker.exists():
            try:
                prev_kept = int(marker.read_text().strip().split("=")[1])
            except (ValueError, IndexError):
                prev_kept = None

        if not args.force and trader_folder_complete(folder):
            print(f"[skip] {name} - already complete")
            saved += 1
            skipped += 1
            try:
                payload = json.loads((folder / f"positions_{MARKET_TYPE}.json").read_text())
                positions_total += len(payload["positions"])
            except (OSError, ValueError, KeyError):
                pass
            continue

        print(f"[{saved + 1}/{limit}] {name} ({trader_id})")

        positions, pos_ok = fetch_history(
            "positionHistory", trader_id, "positions",
            ("positionId", "side", "closedAt", "openedAt"),
        )
        n_pos = len(positions["positions"])

        # Below the bar either way: genuinely too few, or the upstream cannot
        # serve this trader's records at all. Drop the folder so the scan moves
        # on to the next candidate instead of retrying forever.
        if n_pos < MIN_POSITIONS:
            why = "too few" if pos_ok else "incomplete, no data"
            print(f"  {n_pos} positions ({why}) - skipping")
            if folder.exists():
                shutil.rmtree(folder)
            continue

        orders, ord_ok = fetch_history(
            "orderHistory", trader_id, "orders",
            ("createdAt", "symbol", "price", "quantity"),
        )
        profile = api_get("traderProfile", {"traderId": trader_id}) or {}
        open_pos = api_get("openPositions",
                           {"traderId": trader_id, "marketType": MARKET_TYPE}) or {}

        write_json(folder / "profile.json", profile)
        write_json(folder / f"positions_{MARKET_TYPE}.json", positions)
        write_json(folder / f"orders_{MARKET_TYPE}.json", orders)
        write_json(folder / f"open_positions_{MARKET_TYPE}.json", open_pos)

        if pos_ok and ord_ok:
            marker.unlink(missing_ok=True)
        else:
            marker.write_text(f"positions={n_pos}\n")
            if prev_kept is not None and n_pos <= prev_kept:
                print(f"  [accept] {n_pos} rows, no progress vs last attempt - clearing marker")
                marker.unlink()
            else:
                incomplete_traders.append(name)

        saved += 1
        positions_total += n_pos
        flag = "" if (pos_ok and ord_ok) else "  [MARKED INCOMPLETE]"
        print(f"  -> {n_pos} positions, {len(orders['orders'])} orders{flag}")

    write_json(TRADERS_DIR / "_manifest.json", {
        "run_id": run_id,
        "source": SOURCE,
        "date_from": DATE_FROM.isoformat(),
        "date_to": DATE_TO.isoformat(),
        "requested": limit,
        "traders_saved": saved,
        "traders_skipped_existing": skipped,
        "candidates_examined": examined,
        "positions_extracted": positions_total,
        "incomplete_traders": incomplete_traders,
        "extracted_at": extracted_at,
    })

    print(f"\nDone. traders={saved}, positions={positions_total}, examined={examined}")
    if incomplete_traders:
        print(f"INCOMPLETE ({len(incomplete_traders)}): {', '.join(incomplete_traders)}")
        print("Re-run without --force to repair only these.")
    if saved < limit:
        print(f"NOTE: only {saved}/{limit} accepted - pool too thin, "
              f"raise RANKING_LIMIT or lower MIN_POSITIONS.")
    return 0

if __name__ == "__main__":
    sys.exit(main())