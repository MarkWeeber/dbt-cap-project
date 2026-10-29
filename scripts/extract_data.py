#!/usr/bin/env python3
"""
Binance OHLCV downloader -> monthly parquet files, driven entirely by config.py.

Public endpoint only, no API key. Symbols, intervals and the window all come
from scripts/config.py, so the extraction window and the dbt filter window can
never drift apart.

Downloads are always whole months: the day component of DATE_FROM/DATE_TO is
ignored on purpose, so a file is either a complete month or absent.

Idempotent: a month that already has a non-empty file is skipped unless
--force is given. A symbol Binance does not list is dropped once and never
retried for the remaining months.

Usage:
    python3 extract_data.py                  # download everything missing
    python3 extract_data.py --check-symbols  # probe symbols, download nothing
    python3 extract_data.py --force          # re-download even if present
    python3 extract_data.py --symbols BTCUSDT,ETHUSDT
"""

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (                          # noqa: E402
    DATE_FROM, DATE_TO, INTERVALS, MAX_RETRIES, RAW_DIR,
    REQUEST_DELAY, SYMBOLS, TIMEOUT,
)

BASE_URL = "https://api.binance.com"
MAX_CANDLES = 1000
COMPLETED_MONTHS_ONLY = True


class UnsupportedSymbol(Exception):
    """Binance does not list this pair."""


# --------------------------------------------------------------------- window

def last_completed_month_end() -> datetime:
    """Last second of the previous calendar month (UTC)."""
    first_of_this_month = datetime.now(timezone.utc).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0)
    return first_of_this_month - timedelta(seconds=1)


def resolve_range() -> tuple[datetime, datetime]:
    """config dates -> whole-month UTC bounds, clipped to finished months."""
    start = datetime(DATE_FROM.year, DATE_FROM.month, 1, tzinfo=timezone.utc)
    month_after_end = (datetime(DATE_TO.year, DATE_TO.month, 1, tzinfo=timezone.utc)
                       + timedelta(days=32)).replace(day=1)
    end = month_after_end - timedelta(seconds=1)

    if COMPLETED_MONTHS_ONLY:
        end = min(end, last_completed_month_end())

    if start > end:
        raise ValueError(f"window {start:%Y-%m-%d}..{end:%Y-%m-%d} is empty")
    return start, end


# --------------------------------------------------------------------- tasks

@dataclass
class MonthTask:
    """One symbol + interval + month unit of work, owning its own path."""

    symbol: str
    interval: str
    year: int
    month: int

    @property
    def folder(self) -> str:
        return f"{self.year}_{self.month:02d}"

    @property
    def path(self) -> Path:
        return RAW_DIR / self.folder / f"{self.symbol}_{self.interval.upper()}.parquet"


def build_tasks(start: datetime, end: datetime, symbols: list[str]) -> list[MonthTask]:
    tasks, y, m = [], start.year, start.month
    while (y, m) <= (end.year, end.month):
        for symbol in symbols:
            for interval in INTERVALS:
                tasks.append(MonthTask(symbol, interval.lower(), y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return tasks


# ------------------------------------------------------------------ fetching

def get_klines(session, symbol, interval, start_ms, end_ms):
    """One klines request with 429/418 handling. Raises UnsupportedSymbol on 400."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": MAX_CANDLES,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(REQUEST_DELAY)
        r = session.get(f"{BASE_URL}/api/v3/klines", params=params, timeout=TIMEOUT)

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 60))
            print(f"  [rate limit] waiting {wait}s")
            time.sleep(wait)
            continue
        if r.status_code == 418:
            print("  [ip ban] waiting 120s")
            time.sleep(120)
            continue
        if r.status_code == 400:
            try:
                message = r.json().get("msg", "")
            except ValueError:
                message = r.text[:80]
            raise UnsupportedSymbol(message or "invalid request")

        r.raise_for_status()
        return r.json()

    raise RuntimeError(f"klines failed after {MAX_RETRIES} attempts")


def check_symbols(session, symbols: list[str]) -> tuple[list[str], list[str]]:
    """Probe each symbol with a one-candle request. Returns (usable, unsupported)."""
    usable, unsupported = [], []
    for symbol in symbols:
        time.sleep(REQUEST_DELAY)
        try:
            r = session.get(f"{BASE_URL}/api/v3/klines", timeout=TIMEOUT,
                            params={"symbol": symbol, "interval": "1h", "limit": 1})
        except requests.RequestException as exc:
            print(f"  {symbol:16} ERROR  {exc}")
            unsupported.append(symbol)
            continue

        if r.status_code == 200:
            print(f"  {symbol:16} OK     {len(r.json())} candle(s)")
            usable.append(symbol)
            continue

        try:
            reason = r.json().get("msg", r.status_code)
        except ValueError:
            reason = r.status_code
        print(f"  {symbol:16} NO     {reason}")
        unsupported.append(symbol)

    return usable, unsupported


def fetch_month(session, task: MonthTask) -> pd.DataFrame:
    """Paginate one whole month of candles."""
    month_start = datetime(task.year, task.month, 1, tzinfo=timezone.utc)
    month_end = (month_start + timedelta(days=32)).replace(day=1)

    start_ms = int(month_start.timestamp() * 1000)
    end_ms = int(month_end.timestamp() * 1000) - 1

    rows, cursor = [], start_ms
    while cursor < end_ms:
        batch = get_klines(session, task.symbol, task.interval, cursor, end_ms)
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < MAX_CANDLES:
            break
        cursor = batch[-1][0] + 1

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "timestamp", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades",
        "taker_buy_volume", "taker_buy_quote_volume", "ignore",
    ])[["timestamp", "open", "high", "low", "close", "volume"]]

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values("timestamp").reset_index(drop=True)


# ---------------------------------------------------------------------- main

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download Binance OHLCV for the config window.")
    p.add_argument("--check-symbols", action="store_true",
                   help="probe the configured symbols and exit without downloading")
    p.add_argument("--force", action="store_true",
                   help="re-download months that already exist")
    p.add_argument("--symbols", default="",
                   help="comma-separated override instead of config.SYMBOLS")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] or list(SYMBOLS)

    session = requests.Session()

    if args.check_symbols:
        print(f"Probing {len(symbols)} symbol(s)...")
        usable, unsupported = check_symbols(session, symbols)
        print(f"\nusable={len(usable)} unsupported={len(unsupported)}")
        if unsupported:
            print("remove from config.SYMBOLS: " + ", ".join(unsupported))
            return 2
        return 0

    start, end = resolve_range()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"range   : {start:%Y-%m-%d} -> {end:%Y-%m-%d} (whole months)")
    print(f"symbols : {len(symbols)}")
    print(f"saving  : {RAW_DIR}")

    tasks = build_tasks(start, end, symbols)
    print(f"tasks   : {len(tasks)}\n")

    saved = skipped = failed = 0
    dead: set[str] = set()

    for i, task in enumerate(tasks, 1):
        if task.symbol in dead:
            continue

        label = f"{task.symbol} {task.interval} {task.folder}"
        if not args.force and task.path.exists() and task.path.stat().st_size > 0:
            print(f"[{i}/{len(tasks)}] {label} - exists, skip")
            skipped += 1
            continue

        print(f"[{i}/{len(tasks)}] {label} - fetching")
        try:
            df = fetch_month(session, task)
        except UnsupportedSymbol as exc:
            print(f"  unsupported ({exc}) - dropping the rest of this symbol")
            dead.add(task.symbol)
            continue
        except (requests.RequestException, RuntimeError) as exc:
            print(f"  FAILED: {exc}")
            failed += 1
            continue

        if df.empty:
            print("  no data")
            skipped += 1
            continue

        task.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(task.path, engine="pyarrow", compression="snappy", index=False)
        print(f"  saved {len(df)} rows -> {task.path.name}")
        saved += 1

    print(f"\nDone. saved={saved} skipped={skipped} failed={failed}")
    if dead:
        print("unsupported symbols: " + ", ".join(sorted(dead)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())