#!/usr/bin/env python3
"""
Binance Smart Money (Leaderboard) Data Extractor  — fixed
Uses Kopyon API v5: https://kopyon.com/binance-smart-money-api/docs/
"""

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# =============================================================================
# CONFIG
# =============================================================================

# Лучше через env: export KOPYON_API_KEY="kpy_..."
KOPYON_API_KEY = os.getenv("KOPYON_API_KEY", "kpy_ARSpZqyTa6Xk7Iy0W2sYN5xINZ-lz_jN")

SAVE_PATH = Path(__file__).resolve().parent.parent / "data" / "raw" / "leaderboard"

# ВАЛИДНЫЕ значения period у Kopyon (НЕ 1W/1M/3M):
TIMEFRAMES = ["1D", "7D", "30D", "90D", "ALL"]
METRICS = ["PNL", "ROI"]
MARKET_TYPES = ["UM"]          # "UM" = USDⓈ-M, "CM" = COIN-M
PRIMARY_PERIOD = "30D"         # чем ранжируем топ для деталей
PRIMARY_METRIC = "PNL"

REQUEST_DELAY = 0.5            # free tier: 120 req/min на эндпоинт
MAX_RETRIES = 3
TIMEOUT = 30
ROWS_PER_PAGE = 20             # максимум у API
MAX_RANK_PAGES = 8             # сколько страниц рейтинга листать в поисках латиницы
TOP_N = 10
HISTORY_PAGES = 3              # по 20 записей за страницу на трейдера

BASE_URL = "https://api.kopyon.com/v5/binance-smart-money"
HEADERS = {"X-API-KEY": KOPYON_API_KEY}

# Только латиница/цифры/пробел/._- и минимум одна буква
LATIN_RE = re.compile(r"^[A-Za-z0-9 ._\-]+$")


def is_latin_name(name: str) -> bool:
    if not name:
        return False
    name = name.strip()
    if not LATIN_RE.match(name):
        return False
    return any(c.isalpha() and c.isascii() for c in name)


def _get(path, params):
    """GET с ретраями. Возвращает dict data или None."""
    url = f"{BASE_URL}/{path}"
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
            if r.status_code == 401:
                print("  ERROR: неверный API key")
                return None
            if r.status_code == 429:
                print("  Rate limit / quota — ждём 60с")
                time.sleep(60)
                continue
            if r.status_code == 422:
                print(f"  422 INVALID_REQUEST: {r.text[:200]}")
                return None
            r.raise_for_status()
            return r.json().get("data")
        except requests.exceptions.RequestException as e:
            print(f"  Попытка {attempt + 1} не удалась: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
    return None


# =============================================================================
# API
# =============================================================================

def get_traders(ranking_type="PNL", period="30D", pages=MAX_RANK_PAGES):
    """Рейтинг с пагинацией. Возвращает список трейдеров."""
    out, page = [], 1
    while page <= pages:
        data = _get("traders", {
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
        out.extend(traders)
        if not data.get("hasMore") or not traders:
            break
        page += 1
        time.sleep(REQUEST_DELAY)
    return out


def get_open_positions(trader_id, market_type="UM"):
    data = _get("openPositions", {"traderId": trader_id, "marketType": market_type})
    if not data:
        return {"positions": [], "hidden": None}
    return {"positions": data.get("positions", []), "hidden": data.get("hidden")}


def get_position_history(trader_id, market_type="UM", pages=HISTORY_PAGES):
    out, cursor = [], None
    for _ in range(pages):
        params = {"traderId": trader_id, "marketType": market_type, "rows": ROWS_PER_PAGE}
        if cursor:
            params["cursor"] = cursor
        data = _get("positionHistory", params)
        if not data:
            break
        out.extend(data.get("positions", []))
        cursor = data.get("nextCursor")
        if not cursor:
            break
        time.sleep(REQUEST_DELAY)
    return out


def get_order_history(trader_id, market_type="UM", pages=HISTORY_PAGES):
    out, cursor = [], None
    for _ in range(pages):
        params = {"traderId": trader_id, "marketType": market_type, "rows": ROWS_PER_PAGE}
        if cursor:
            params["cursor"] = cursor
        data = _get("orderHistory", params)
        if not data:
            break
        out.extend(data.get("orders", []))
        cursor = data.get("nextCursor")
        if not cursor:
            break
        time.sleep(REQUEST_DELAY)
    return out


# =============================================================================
# ЛОГИКА
# =============================================================================

def fetch_all_leaderboards():
    SAVE_PATH.mkdir(parents=True, exist_ok=True)
    all_data = {}
    for period in TIMEFRAMES:
        for metric in METRICS:
            print(f"Рейтинг {period} {metric}...")
            traders = get_traders(ranking_type=metric, period=period)
            key = f"{period.lower()}_{metric.lower()}"
            all_data[key] = traders
            print(f"  -> {len(traders)} трейдеров")
            filepath = SAVE_PATH / f"leaderboard_{key}.json"
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump({
                    "metadata": {
                        "period": period, "metric": metric,
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                        "count": len(traders), "source": "kopyon_api",
                    },
                    "data": traders,
                }, f, indent=2, ensure_ascii=False)
            time.sleep(REQUEST_DELAY)
    return all_data


def pick_top_latin(all_data, top_n=TOP_N):
    key = f"{PRIMARY_PERIOD.lower()}_{PRIMARY_METRIC.lower()}"
    traders = all_data.get(key) or []
    picked = [t for t in traders if is_latin_name(t.get("name", ""))]
    return picked[:top_n]


def fetch_top_trader_details(all_data, top_n=TOP_N):
    top_traders = pick_top_latin(all_data, top_n)
    print(f"\nНайдено латинских имён в топе: {len(top_traders)}")
    if not top_traders:
        print("Латиницы не нашлось — увеличь MAX_RANK_PAGES")
        return

    details_path = SAVE_PATH / "trader_details"
    details_path.mkdir(exist_ok=True)

    for i, trader in enumerate(top_traders, 1):
        trader_id = trader.get("traderId")
        name = trader.get("name", "unknown")
        print(f"[{i}/{len(top_traders)}] {name} ({trader_id})")

        markets = {}
        for mt in MARKET_TYPES:
            positions = get_open_positions(trader_id, mt)
            time.sleep(REQUEST_DELAY)
            history = get_position_history(trader_id, mt)
            time.sleep(REQUEST_DELAY)
            orders = get_order_history(trader_id, mt)
            time.sleep(REQUEST_DELAY)
            markets[mt] = {
                "open_positions": positions["positions"],
                "positions_hidden": positions["hidden"],
                "position_history": history,
                "order_history": orders,
            }

        detail = {
            "leaderboard_data": trader,
            "markets": markets,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        safe_name = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")[:30] or "trader"
        filepath = details_path / f"trader_{i:02d}_{safe_name}_{trader_id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(detail, f, indent=2, ensure_ascii=False)
        print(f"  -> {filepath.name}")


def print_summary(all_data):
    print("\n" + "=" * 60 + "\nSUMMARY\n" + "=" * 60)
    for key, traders in all_data.items():
        if not traders:
            continue
        latin = [t for t in traders if is_latin_name(t.get("name", ""))]
        print(f"\n{key.upper()}: всего {len(traders)}, латиницей {len(latin)}")
        for i, t in enumerate(traders[:3], 1):
            roi, pnl = t.get("roi", 0), t.get("pnl", 0)
            roi_pct = roi * 100 if abs(roi) < 10 else roi
            print(f"  {i}. {t.get('name')}: ROI={roi_pct:.2f}% PNL=${pnl:,.0f}")


def main():
    print("=" * 60 + "\nBinance Smart Money Extractor (fixed)\n" + "=" * 60)
    print(f"Output: {SAVE_PATH}\nPeriods: {TIMEFRAMES}\n")

    all_data = fetch_all_leaderboards()
    fetch_top_trader_details(all_data, TOP_N)
    print_summary(all_data)
    print("\nDONE")


if __name__ == "__main__":
    main()