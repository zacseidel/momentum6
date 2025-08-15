# report.py  – fetch & cache company metadata + news  (Python 3.9)

import os, sqlite3, requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
from time import sleep

import pandas as pd
from dotenv import load_dotenv

# ────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────
load_dotenv()
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

DB_PATH  = Path("data/market_data.sqlite")
DB_PATH.parent.mkdir(exist_ok=True)

TTL_DAYS       = int(os.getenv("META_TTL_DAYS", 25))   # refresh metadata weekly
NEWS_LIMIT     = 5
RATE_SLEEP_SEC = 13
FAST_MODE      = os.getenv("FAST_MODE", "0") == "1"   # set FAST_MODE=1 to skip sleeps

# ────────────────────────────────────────────────────────────────────────
# Schema helpers
# ────────────────────────────────────────────────────────────────────────
def ensure_tables() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS company_metadata (
                            ticker      TEXT PRIMARY KEY,
                            name        TEXT,
                            description TEXT,
                            updated_at  TEXT
                        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS company_news (
                            ticker        TEXT,
                            published_utc TEXT,
                            headline      TEXT,
                            url           TEXT,
                            PRIMARY KEY (ticker, published_utc)
                        )""")

# ────────────────────────────────────────────────────────────────────────
# Polygon fetchers
# ────────────────────────────────────────────────────────────────────────
# add near your config
BACKOFF_SEC = 61
MAX_TRIES = 5

_session = requests.Session()
_session.headers.update({"User-Agent": "momentum-screener/1.0"})

def _sleep_polite(seconds: float) -> None:
    # FAST_MODE short-circuits long waits to ~1s for local tests
    sleep(1.0 if FAST_MODE else seconds)

def _get_json_with_backoff(url: str, params: Dict, timeout: int = 15) -> Dict:
    last_exc = None
    for attempt in range(1, MAX_TRIES + 1):
        try:
            r = _session.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 503):
                ra = r.headers.get("Retry-After")
                try:
                    wait = float(ra) if ra is not None else BACKOFF_SEC
                except ValueError:
                    wait = BACKOFF_SEC
                _sleep_polite(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            last_exc = e
            if attempt < MAX_TRIES:
                _sleep_polite(min(5 * attempt, 15))  # brief wait on transient errors
            else:
                break
    raise last_exc if last_exc else RuntimeError("Failed after backoff")






def fetch_company_metadata(ticker: str) -> Dict[str, str]:
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker.upper()}"
    data = _get_json_with_backoff(url, params={"apiKey": POLYGON_API_KEY}).get("results", {})
    return {
        "ticker": ticker.upper(),
        "name": data.get("name"),
        "description": data.get("description"),
        "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
    }

def fetch_company_news(ticker: str, limit: int = NEWS_LIMIT) -> List[Dict[str, str]]:
    url = "https://api.polygon.io/v2/reference/news"
    params = {
        "ticker": ticker.upper(),
        "limit": limit,
        "order": "desc",
        "sort": "published_utc",
        "apiKey": POLYGON_API_KEY,
    }
    data = _get_json_with_backoff(url, params=params)
    return data.get("results", [])

# ────────────────────────────────────────────────────────────────────────
# Public entrypoint
# ────────────────────────────────────────────────────────────────────────
def cache_company_data(tickers: List[str]) -> None:
    """
    Ensure latest metadata + last 5 news items are cached for each ticker.
    Skips metadata if < TTL_DAYS old; skips news if at least one story
    from the past 5 days is already stored.
    """
    ensure_tables()
    tickers = sorted(set(t.upper() for t in tickers))   # de-dup & normalise

    ttl_cutoff = datetime.utcnow() - timedelta(days=TTL_DAYS)
    news_cutoff = datetime.utcnow() - timedelta(days=5)

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        for idx, tkr in enumerate(tickers, 1):
            print(f"🔍  {tkr}  ({idx}/{len(tickers)})")

            # ── metadata freshness check
            cur.execute(
                "SELECT updated_at FROM company_metadata WHERE ticker = ?", (tkr,)
            )
            row = cur.fetchone()
            meta_fresh = bool(row and row[0] and datetime.fromisoformat(row[0]) > ttl_cutoff)


            # ── recent news check
            cur.execute(
                "SELECT COUNT(*) FROM company_news "
                "WHERE ticker = ? AND published_utc > ?",
                (tkr, news_cutoff.isoformat()),
            )
            has_recent_news = cur.fetchone()[0] > 0

            # ── fetch metadata if stale
            if not meta_fresh:
                print("   🧠  Fetching metadata …")
                meta = fetch_company_metadata(tkr)
                cur.execute(
                    """INSERT OR REPLACE INTO company_metadata
                       (ticker, name, description, updated_at)
                       VALUES (:ticker,:name,:description,:updated_at)""",
                    meta,
                )
                conn.commit()
                if not FAST_MODE:
                    sleep(RATE_SLEEP_SEC)

            # ── fetch news if none this week
            if not has_recent_news:
                print("   📰  Fetching news …")
                for item in fetch_company_news(tkr):
                    cur.execute(
                        """INSERT OR IGNORE INTO company_news
                           (ticker, published_utc, headline, url)
                           VALUES (?, ?, ?, ?)""",
                        (
                            tkr,
                            item.get("published_utc"),
                            item.get("title"),
                            item.get("article_url"),
                        ),
                    )
                conn.commit()
                if not FAST_MODE:
                    sleep(RATE_SLEEP_SEC)
            else:
                print("   ✅  Recent news present – skip API")

# ────────────────────────────────────────────────────────────────────────
# CLI helper (manual run)
# ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    cache_company_data(["AAPL", "MSFT", "GOOGL"])
