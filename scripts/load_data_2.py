#!/usr/bin/env python3
"""
Load parquet files from data/raw into the DuckDB RAW schema.

Part of the ELT pipeline (Extract -> Load -> Transform). Full-refresh and
idempotent: the whole RAW schema is dropped and rebuilt inside one
transaction, so a failure rolls back to the previous good state. Only the
RAW schema is ever touched.
"""

import re
import sys
from collections import defaultdict
from pathlib import Path

import duckdb

# =============================================================================
# CONFIGURATION
# =============================================================================

# Anchored to the script location so it works regardless of the CWD.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = PROJECT_ROOT / "dev.duckdb"   # referenced by dbt profiles.yml
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"
RAW_SCHEMA = "RAW"

# Column layout written by the extractor.
COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]
COLUMN_DDL = (
    "timestamp TIMESTAMPTZ, open DOUBLE, high DOUBLE, "
    "low DOUBLE, close DOUBLE, volume DOUBLE"
)


# =============================================================================
# DISCOVERY
# =============================================================================

def interval_suffix(interval: str) -> str:
    """Map a Binance interval to the table suffix: '1d' -> 'D1', '15m' -> 'M15'."""
    match = re.match(r"^(\d+)([a-zA-Z]+)$", interval)
    if not match:
        raise ValueError(f"Unrecognised interval: {interval!r}")
    number, unit = match.groups()
    return f"{unit.upper()}{number}"


def discover() -> dict[tuple[str, str], list[Path]]:
    """
    Recursively find parquet files and group them by (symbol, suffix).

    Filenames look like SYMBOL_INTERVAL.parquet (e.g. BTCUSDT_1D.parquet).
    Each group becomes one RAW_<SYMBOL>_<SUFFIX> table, so a table only
    exists if at least one matching parquet was actually downloaded.
    """
    groups: dict[tuple[str, str], list[Path]] = defaultdict(list)
    for path in sorted(RAW_DATA_PATH.rglob("*.parquet")):
        symbol, _, interval = path.stem.partition("_")
        groups[(symbol.upper(), interval_suffix(interval))].append(path)
    return groups


# =============================================================================
# LOAD
# =============================================================================

def create_and_load(conn, table: str, files: list[Path]) -> int:
    """Create a RAW table with explicit types and insert rows ordered by timestamp."""
    conn.execute(f'CREATE TABLE {RAW_SCHEMA}."{table}" ({COLUMN_DDL})')

    paths = "[" + ", ".join(f"'{p}'" for p in files) + "]"
    conn.execute(f"""
        INSERT INTO {RAW_SCHEMA}."{table}" ({", ".join(COLUMNS)})
        SELECT {", ".join(COLUMNS)}
        FROM read_parquet({paths})
        ORDER BY timestamp
    """)

    return conn.execute(f'SELECT COUNT(*) FROM {RAW_SCHEMA}."{table}"').fetchone()[0]


def main() -> int:
    if not RAW_DATA_PATH.exists():
        print(f"No data directory: {RAW_DATA_PATH} - run the extract script first.")
        return 1

    groups = discover()
    if not groups:
        print(f"No parquet files found under {RAW_DATA_PATH}.")
        return 1

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DUCKDB_PATH))

    try:
        # Rebuild RAW atomically: either the whole schema reloads, or nothing changes.
        conn.execute("BEGIN TRANSACTION")
        conn.execute(f"DROP SCHEMA IF EXISTS {RAW_SCHEMA} CASCADE")
        conn.execute(f"CREATE SCHEMA {RAW_SCHEMA}")

        total = 0
        for (symbol, suffix) in sorted(groups):
            table = f"RAW_{symbol}_{suffix}"
            rows = create_and_load(conn, table, groups[(symbol, suffix)])
            total += rows
            print(f"{table}: {len(groups[(symbol, suffix)])} file(s), {rows} rows")

        conn.execute("COMMIT")
        print(f"\nLoaded {len(groups)} table(s), {total} rows -> {DUCKDB_PATH}")

    except Exception as e:
        conn.execute("ROLLBACK")
        print(f"FAILED, rolled back: {e}")
        return 1
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())