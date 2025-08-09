# ranking.py  – v2 (uses universe + lean DB)

import os, sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Dict

import pandas as pd
from pandas.tseries.offsets import BDay

from universe import UniverseService   # NEW

# ----------------------------------------------------------------------
DB_PATH = Path("data/market_data.sqlite")

# ----------------------------------------------------------------------
# 1. Fetch price snapshots for the given cohort
# ----------------------------------------------------------------------
def get_price_snapshots(target_dates: Dict[str, str],
                        *,
                        index_type: str = "sp500",
                        db_path: Optional[Path] = None):
    """
    Return a pivoted DataFrame (rows = ticker) with close prices on all
    anchor dates + a dict of the actual dates we found in the DB.

    index_type ∈ {'sp500', 'sp400', 'megacap'}
    """

    uni = UniverseService()
    tickers = uni.get_cohort(index_type)["symbol"].tolist()
    def _backtrack_to_available(conn, date_str):
        d = pd.to_datetime(date_str)
        for _ in range(7):                       # look back max 1 week
            test = d.strftime("%Y-%m-%d")
            if conn.execute(
                "SELECT 1 FROM daily_prices WHERE date = ? LIMIT 1", (test,)
            ).fetchone():
                return test
            d -= BDay(1)
        raise ValueError(f"No price data found near {date_str}")

    db_path = db_path or DB_PATH
    with sqlite3.connect(db_path) as conn:
        resolved = {
            label: _backtrack_to_available(conn, ds)
            for label, ds in target_dates.items()
        }
        placeholders = ",".join("?" * len(resolved))
        rows = pd.read_sql(
            f"SELECT ticker, date, close "
            f"FROM   daily_prices "
            f"WHERE  date IN ({placeholders})",
            conn,
            params=list(resolved.values()),
        )

    df = rows.pivot(index="ticker", columns="date", values="close")
    df = df.reindex(index=tickers)               # ensure order & missing tickers
    df.columns.name = None
    return df, resolved


# ----------------------------------------------------------------------
# 2. Compute returns + ranks (unchanged except Optional hints)
# ----------------------------------------------------------------------
def compute_returns_and_ranks(df: pd.DataFrame,
                              target_dates: Dict[str, str]) -> pd.DataFrame:
    c_now      = df[target_dates["yesterday"]]
    c_lastweek = df[target_dates["week_ago_yesterday"]]
    c_1y       = df[target_dates["one_year_ago"]]
    c_1mo      = df[target_dates["one_month_ago"]]
    c_1y_1mo   = df[target_dates["one_year_plus_month"]]

    current_return   = (c_now - c_1y) / c_1y
    previous_return  = (c_1mo - c_1y_1mo) / c_1y_1mo
    last_week_return = (c_now - c_lastweek) / c_lastweek

    current_rank  = current_return.rank(ascending=False, method="min")
    previous_rank = previous_return.rank(ascending=False, method="min")
    rank_change   = previous_rank - current_rank

    res = pd.DataFrame({
        "current_return":   current_return,
        "last_week_return": last_week_return,
        "last_month_return": previous_return,
        "current_rank":     current_rank,
        "last_month_rank":  previous_rank,
        "rank_change":      rank_change,
    }).dropna()

    res = res[res["current_rank"] <= res["last_month_rank"]]           # improving/steady
    res = res.sort_values("current_return", ascending=False)

    pct = lambda x: f"{x * 100:.1f}%"
    for col in ("current_return", "last_week_return", "last_month_return"):
        res[col] = res[col].map(pct)

    return res


# ----------------------------------------------------------------------
# 3. Helpers to store weekly Top-10 tables
# ----------------------------------------------------------------------
_TOP10_TABLES = {
    "sp500": "top10_spy",
    "sp400": "top10_mdy",
    "megacap": "top10_mega",
}

def _store_top10_generic(result: pd.DataFrame,
                         index_type: str,
                         run_date: Optional[str] = None,
                         db_path: Optional[Path] = None):
    if result.empty:
        print(f"⚠️  No {index_type} results to store.")
        return pd.DataFrame()

    top10 = result[result["rank_change"] >= 0].head(10).copy()
    if run_date is None:
        run_date = pd.Timestamp.today().date().isoformat()
    elif isinstance(run_date, (pd.Timestamp, datetime)):
        run_date = run_date.date().isoformat()
    else:  # already a str
        run_date = str(run_date)
    top10["date"] = run_date
    top10 = top10.reset_index().rename(columns={"ticker": "ticker"})

    table = _TOP10_TABLES[index_type]
    db_path = db_path or DB_PATH
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"DELETE FROM {table} WHERE date = ?", (run_date,))
        top10.to_sql(table, conn, if_exists="append", index=False)
        print(f"✅ Stored {index_type} top-10 for {run_date}")

    return top10

# Public wrappers
def store_top10_picks(res, run_date=None, db_path=DB_PATH):
    return _store_top10_generic(res, "sp500", run_date, db_path)

def store_top10_mdy_picks(res, run_date=None, db_path=DB_PATH):
    return _store_top10_generic(res, "sp400", run_date, db_path)

def store_top10_mega_picks(res, run_date=None, db_path=DB_PATH):
    return _store_top10_generic(res, "megacap", run_date, db_path)


# ----------------------------------------------------------------------
# 4. Manual CLI test
# ----------------------------------------------------------------------
if __name__ == "__main__":
    from prices import get_target_dates
    dates = get_target_dates()
    df, resolved = get_price_snapshots(dates)             # SP500 by default
    ranks = compute_returns_and_ranks(df, resolved)
    store_top10_picks(ranks)
