"""Single source of truth for the whole pipeline.

Change the window or the symbol list HERE and every extractor picks it up.
Keep it to constants only - no business logic.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")     # no-op when the shell already exports it
except ImportError:
    pass

# --------------------------------------------------------------------- paths
DATA_DIR = REPO_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
TRADERS_DIR = RAW_DIR / "traders"
PROBE_DIR = RAW_DIR / "_probe"
SEEDS_DIR = REPO_ROOT / "dbt_project" / "seeds"

# --------------------------------------------------------------------- window
# The extraction window and the dbt filter window MUST match.
DATE_FROM = date(2026, 1, 1)
DATE_TO = date(2026, 9, 30)

# --------------------------------------------------------------------- market
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AAVEUSDT", "TWTUSDT", "BNBUSDT", "LTCUSDT", "XRPUSDT"]
INTERVALS = ["1h"]
MARKET_TYPE = "UM"          # UM = USDS-M futures, CM = COIN-M futures

# --------------------------------------------------------------------- traders
N_TRADERS = 20
MIN_POSITIONS = 50          # skip traders with fewer closed positions than this
CHUNK_DAYS = 30             # the API caps position history at 501 rows per query
RANKING_PERIOD = "90D"      # ranking period used to pick the traders

# --------------------------------------------------------------------- api
API_BASE = "https://api.kopyon.com"
API_KEY_ENV = "KOPYON_API_KEY"
REQUEST_DELAY = 0.4
TIMEOUT = 30
MAX_RETRIES = 4
RATE_LIMIT_PER_MIN = 120


# --------------------------------------------------------------------- helpers
def to_ms(d: date) -> int:
    """date -> UTC Unix milliseconds, the format the API expects."""
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def date_chunks(start: date = DATE_FROM, end: date = DATE_TO, days: int = CHUNK_DAYS):
    """Split the window into slices of `days` days.

    Without this a single wide-range request silently returns only the first
    501 positions and the rest is lost.
    """
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=days - 1), end)
        yield cur, nxt
        cur = nxt + timedelta(days=1)