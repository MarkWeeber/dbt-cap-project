#!/usr/bin/env python3
"""
Binance OHLCV downloader -> monthly parquet files (DuckDB-friendly).

Public API only (no keys). Idempotent: existing monthly files are skipped.
Only fully completed months are ever written. Downloads are always whole
months - any day component in the config dates is ignored.
"""

import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

# =============================================================================
# CONFIGURATION - modify these values
# =============================================================================

# Robust to the directory you run from (no more "../" CWD surprises).
SAVE_PATH = Path(__file__).resolve().parent.parent / "data" / "raw"

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
INTERVALS = ["1h", "1d"]

DATE_FROM = "2026-08"   # YYYY | YYYY-MM | YYYY-MM-DD (day is ignored)
DATE_TO = ""            # empty = end of last completed month

BASE_URL = "https://api.binance.com"
MAX_CANDLES = 1000
REQUEST_DELAY = 0.25    # seconds between requests (safe for weight-5 calls)
MAX_RETRIES = 4
COMPLETED_MONTHS_ONLY = True


# =============================================================================
# DATE HELPERS
# =============================================================================

def parse_date(value: str, is_end: bool = False) -> datetime:
    """
    Parse YYYY, YYYY-MM or YYYY-MM-DD and snap to a whole-month boundary.

    Any day component is discarded on purpose: the result is always the first
    instant of the month (is_end=False) or the last instant of the month
    (is_end=True). This guarantees every downloaded file is a complete month.
    """
    parts = value.replace("/", "-").split("-")
    year = int(parts[0])
    month = int(parts[1]) if len(parts) > 1 else (12 if is_end else 1)

    if is_end:
        # First day of the next month, minus one day = last day of this month.
        next_month = (datetime(year, month, 1, tzinfo=timezone.utc) + timedelta(days=32)).replace(day=1)
        return (next_month - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    return datetime(year, month, 1, tzinfo=timezone.utc)


def last_completed_month_end() -> datetime:
    """End of the previous calendar month (UTC)."""
    now = datetime.now(timezone.utc)
    last_day = datetime(now.year, now.month, 1, tzinfo=timezone.utc) - timedelta(days=1)
    return last_day.replace(hour=23, minute=59, second=59)


def get_date_range() -> tuple[datetime, datetime]:
    start = parse_date(DATE_FROM)
    end = parse_date(DATE_TO, is_end=True) if DATE_TO.strip() else last_completed_month_end()

    if COMPLETED_MONTHS_ONLY:
        end = min(end, last_completed_month_end())

    if start > end:
        raise ValueError(f"DATE_FROM {start:%Y-%m-%d} is after DATE_TO {end:%Y-%m-%d}")

    print(f"Date range: {start:%Y-%m-%d} -> {end:%Y-%m-%d}")
    return start, end


# =============================================================================
# TASK PLANNING
# =============================================================================

@dataclass
class MonthTask:
    """
    One unit of work: a single symbol + interval + month combination.

    The task owns its own destination path, so the rest of the script never
    has to re-derive folder/file names. Folder layout is data/raw/YYYY_MM/ and
    the file is SYMBOL_INTERVAL.parquet - one file per month keeps the DuckDB
    partition granularity at month level.
    """
    symbol: str
    interval: str
    year: int
    month: int

    @property
    def folder(self) -> str:
        """Month partition folder name, e.g. '2026_08'."""
        return f"{self.year}_{self.month:02d}"

    @property
    def path(self) -> Path:
        """Full parquet path for this task, e.g. data/raw/2026_08/BTCUSDT_1H.parquet."""
        return SAVE_PATH / self.folder / f"{self.symbol}_{self.interval.upper()}.parquet"


def build_tasks(start: datetime, end: datetime) -> list[MonthTask]:
    """
    Expand a date range into every symbol x interval x month task.

    Walks month by month from start to end (inclusive) and, for each month,
    produces one MonthTask per configured symbol and interval. Uses a simple
    (year, month) tuple comparison for the loop bound and rolls the month over
    manually so no date-arithmetic library is needed.
    """
    tasks, y, m = [], start.year, start.month
    while (y, m) <= (end.year, end.month):
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                tasks.append(MonthTask(symbol, interval.lower(), y, m))
        # Advance one month, rolling December into January of the next year.
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return tasks


# =============================================================================
# BINANCE FETCH
# =============================================================================

def get_klines(session, symbol, interval, start_ms, end_ms):
    """One klines request with 429/418 handling and backoff."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": MAX_CANDLES,
    }
    for attempt in range(MAX_RETRIES):
        time.sleep(REQUEST_DELAY)
        r = session.get(f"{BASE_URL}/api/v3/klines", params=params, timeout=30)

        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 60)))
            continue
        if r.status_code == 418:
            time.sleep(120)
            continue
        r.raise_for_status()
        return r.json()

    raise RuntimeError(f"klines failed after {MAX_RETRIES} attempts")


def fetch_month(session, task: MonthTask) -> pd.DataFrame:
    """Paginate one full month of candles for a task."""
    month_start = datetime(task.year, task.month, 1, tzinfo=timezone.utc)
    month_end = (datetime(task.year, task.month, 1, tzinfo=timezone.utc) + timedelta(days=32)).replace(day=1)

    start_ms = int(month_start.timestamp() * 1000)
    end_ms = int(month_end.timestamp() * 1000) - 1

    rows = []
    cursor = start_ms
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
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values("timestamp").reset_index(drop=True)


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:
    start, end = get_date_range()
    SAVE_PATH.mkdir(parents=True, exist_ok=True)
    print(f"Saving to: {SAVE_PATH}")

    session = requests.Session()
    tasks = build_tasks(start, end)

    done = skipped = failed = 0
    for i, task in enumerate(tasks, 1):
        label = f"{task.symbol} {task.interval} {task.folder}"
        if task.path.exists() and task.path.stat().st_size > 0:
            print(f"[{i}/{len(tasks)}] {label} - exists, skip")
            skipped += 1
            continue

        print(f"[{i}/{len(tasks)}] {label} - fetching")
        try:
            df = fetch_month(session, task)
            if df.empty:
                print("  no data")
                skipped += 1
                continue

            task.path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(task.path, engine="pyarrow", compression="snappy", index=False)
            print(f"  saved {len(df)} rows -> {task.path}")
            done += 1
        except Exception as e:
            print(f"  FAILED: {e}")
            failed += 1

    print(f"\nDone. saved={done} skipped={skipped} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())