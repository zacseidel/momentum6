# prices.py – v2 (uses universe & lean DB schema)

import os, sqlite3, requests, io
from datetime import date, timedelta
from typing import Optional, Tuple
from pathlib import Path
from time import sleep

import pandas as pd
from dotenv import load_dotenv

from universe import UniverseService

load_dotenv()
API_KEY = os.getenv("POLYGON_API_KEY")
DB_PATH  = Path("data/market_data.sqlite")

# ────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ────────────────────────────────────────────────────────────────────────────
def _ensure_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS daily_prices (
                            ticker TEXT,
                            date   DATE,
                            open   REAL, high REAL, low REAL,
                            close  REAL, volume INTEGER,
                            PRIMARY KEY (ticker,date)
                        )""")

def _last_trading_thursday(today: Optional[date] = None) -> date:
    today = today or date.today()
    thu = today - timedelta(days=(today.weekday() - 3) % 7)
    return thu  # holiday handling left to Polygon (will return 0 rows)


def _fetch_grouped_for_date(d: date, *, adjusted: bool = True, timeout: int = 30) -> list:
    url = (
        f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
        f"{d:%Y-%m-%d}?adjusted={'true' if adjusted else 'false'}&apiKey={API_KEY}"
    )
    r = requests.get(url, timeout=timeout)
    if r.status_code == 429:
        # gentle handling: wait a minute and let caller retry same date
        time.sleep(65)
        return _fetch_grouped_for_date(d, adjusted=adjusted, timeout=timeout)
    r.raise_for_status()
    return r.json().get("results", []) or []


def backtrack_grouped_to_available(
    anchor: date,
    *,
    max_weekdays_back: int = 5,
    adjusted: bool = True,
    verbose: bool = True,
    sleep_between: float = 13.0,  # respect free-tier (5 req/min)
) -> Tuple[date, list]:
    """
    Try `anchor`; if empty, step backward one *weekday* at a time (skip Sat/Sun),
    up to `max_weekdays_back` weekday attempts. Returns (resolved_date, results).
    Raises RuntimeError if nothing found.
    """
    if not API_KEY:
        raise RuntimeError("POLYGON_API_KEY is not set")

    attempts_used = 0
    d = anchor

    while True:
        # Skip weekends without burning an attempt
        if d.weekday() >= 5:
            d -= timedelta(days=1)
            continue

        if verbose:
            print(f"⬇️  Fetching grouped bars for {d} …")
        data = _fetch_grouped_for_date(d, adjusted=adjusted)
        if verbose:
            print(f"🔍 Polygon returned {len(data)} rows for {d}")

        if data:
            return d, data

        # no data on this weekday → move back
        attempts_used += 1
        if attempts_used > max_weekdays_back:
            raise RuntimeError(
                f"No grouped data found within {max_weekdays_back} weekdays before {anchor}"
            )
        d -= timedelta(days=1)
        if sleep_between:
            sleep(sleep_between)


# ────────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────────

def sync_grouped_bars(as_of: Optional[date] = None, *, verbose=True, max_back_weekdays: int = 5) -> date:
    """
    Pull grouped bars for the last-Thursday anchor; if empty, backtrack to the
    most recent available *weekday* (skipping weekends) up to `max_back_weekdays`.
    Inserts rows *only for tickers in the current universe*.
    Returns the resolved trading date actually used.
    """
    as_of = as_of or date.today()
    target = _last_trading_thursday(as_of)

    _ensure_db()

    # Build universe set
    uni = UniverseService()
    symbols = set(uni.get_cohort("sp500")["symbol"]) | set(uni.get_cohort("sp400")["symbol"])

    if verbose:
        print(f"💡 Universe tickers loaded: {len(symbols)}")

    # If already populated for a very similar date (same resolved date), skip.
    # Note: we don't know resolved date yet; first check the *anchor* to avoid redundant work
    with sqlite3.connect(DB_PATH) as conn:
        cnt_anchor = conn.execute(
            "SELECT COUNT(*) FROM daily_prices WHERE date = ?", (target.isoformat(),)
        ).fetchone()[0]
    if cnt_anchor > 0.9 * len(symbols):
        if verbose:
            print(f"✅ daily_prices already populated for anchor {target} ({cnt_anchor} rows)")
        # Still make sure we have the index ETF close
        _store_single_ticker_close("VOO", target, verbose=verbose, max_back_weekdays=max_back_weekdays)
        return target

    # Backtrack to an available weekday
    resolved_date, data = backtrack_grouped_to_available(
        target, max_weekdays_back=max_back_weekdays, verbose=verbose
    )

    # If we landed on a different date, check if DB already has it
    with sqlite3.connect(DB_PATH) as conn:
        cnt_resolved = conn.execute(
            "SELECT COUNT(*) FROM daily_prices WHERE date = ?", (resolved_date.isoformat(),)
        ).fetchone()[0]
    if cnt_resolved > 0.9 * len(symbols):
        if verbose:
            print(f"✅ daily_prices already populated for resolved {resolved_date} ({cnt_resolved} rows)")
        _store_single_ticker_close("VOO", resolved_date, verbose=verbose, max_back_weekdays=max_back_weekdays)
        return resolved_date

    # Convert → DataFrame → filter universe
    df = (
        pd.DataFrame(data)[["T", "o", "h", "l", "c", "v"]]
        .rename(columns={"T": "ticker", "o": "open", "h": "high",
                         "l": "low", "c": "close", "v": "volume"})
    )
    df = df[df["ticker"].isin(symbols)].copy()
    df["date"] = resolved_date.isoformat()

    if verbose:
        print(f"📄 Rows after universe filter: {len(df)}")

    # Insert (ignore duplicates quietly in case of reruns)
    with sqlite3.connect(DB_PATH) as conn:
        # Use a temp table to avoid IntegrityError storms with to_sql
        tmp = "_tmp_prices"
        df.to_sql(tmp, conn, if_exists="replace", index=False)
        conn.execute("""
            INSERT OR IGNORE INTO daily_prices (ticker,date,open,high,low,close,volume)
            SELECT ticker,date,open,high,low,close,volume FROM _tmp_prices
        """)
        conn.execute("DROP TABLE IF EXISTS _tmp_prices")
    if verbose:
        print(f"💾 Stored {len(df)} rows for {resolved_date}")

    # Make sure we also store the index ETF close near that date
    _store_single_ticker_close("VOO", resolved_date, verbose=verbose, max_back_weekdays=max_back_weekdays)

    return resolved_date




# ---------------------------------------------------------------------------

# ­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­
# Single-ticker OHLCV for ETFs / indexes that don’t appear in grouped bars
# ---------------------------------------------------------------------------

def _store_single_ticker_close(
    ticker: str,
    dt: date,
    *,
    verbose: bool = True,
    max_back_weekdays: int = 5,
) -> Optional[date]:
    """
    Fetch 1-day OHLCV for *ticker* on `dt`. If missing, backtrack up to
    `max_back_weekdays` weekdays. Inserts the bar for the date actually found.
    Returns the date used, or None if nothing found.
    """
    # Skip if already present on dt
    iso = dt.isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM daily_prices WHERE ticker=? AND date=?", (ticker, iso)
        ).fetchone()
        if row:
            return dt

    # Try dt, then backtrack weekdays
    attempts_used = 0
    cur = dt
    while True:
        if cur.weekday() >= 5:  # weekend → skip without burning attempt
            cur -= timedelta(days=1)
            continue

        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
            f"{cur:%Y-%m-%d}/{cur:%Y-%m-%d}?adjusted=true&apiKey={API_KEY}"
        )
        r = requests.get(url, timeout=20)
        if r.status_code == 429:
            time.sleep(65)
            continue
        r.raise_for_status()
        res = r.json().get("results", []) or []
        if res:
            bar = res[0]
            rec = (ticker, cur.isoformat(), bar["o"], bar["h"], bar["l"], bar["c"], bar["v"])
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute("""INSERT OR IGNORE INTO daily_prices
                                (ticker,date,open,high,low,close,volume)
                                VALUES (?,?,?,?,?,?,?)""", rec)
            if verbose:
                print(f"💾 Stored {ticker} close for {cur:%Y-%m-%d}: ${bar['c']:.2f}")
            return cur

        # no bar → backtrack
        attempts_used += 1
        if attempts_used > max_back_weekdays:
            if verbose:
                print(f"⚠️  Polygon had no data for {ticker} within {max_back_weekdays} weekdays before {dt}")
            return None
        cur -= timedelta(days=1)
        sleep(13)  # respect free tier



# ---------------------------------------------------------------------------
# Date helpers reused by ranking code (kept from v1 – unchanged)
# ---------------------------------------------------------------------------
def get_target_dates(today: Optional[date] = None):
    today = pd.Timestamp(today or date.today())
    one_d  = today - pd.Timedelta(days=1)
    one_w  = one_d - pd.DateOffset(weeks=1)
    one_m  = one_d - pd.DateOffset(months=1)
    one_y  = one_d - pd.DateOffset(years=1)
    one_y_plus_m = one_m - pd.DateOffset(years=1)
    fmt = "%Y-%m-%d"
    return {
        "yesterday":            one_d.strftime(fmt),
        "week_ago_yesterday":   one_w.strftime(fmt),
        "one_month_ago":        one_m.strftime(fmt),
        "one_year_ago":         one_y.strftime(fmt),
        "one_year_plus_month":  one_y_plus_m.strftime(fmt),
    }

# For quick manual testing
if __name__ == "__main__":
    sync_grouped_bars(verbose=True)
