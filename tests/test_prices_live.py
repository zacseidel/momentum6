# tests/test_prices_live.py
"""
Live Polygon integration test for prices.sync_grouped_bars.

• Reads POLYGON_API_KEY from .env (root) or env-vars.
• Uses a temp SQLite DB so prod data stay clean.
"""

import os, sqlite3, asyncio
from pathlib import Path
from datetime import date

import pandas as pd
from dotenv import load_dotenv
import pytest

import prices
from universe import UniverseService

# ── 1.  Load the root .env so POLYGON_API_KEY is available
load_dotenv()                                     # NEW

@pytest.mark.skipif(
    not os.getenv("POLYGON_API_KEY"),
    reason="POLYGON_API_KEY not configured – skipping live API test",
)
def test_sync_grouped_bars_live(tmp_path):
    # 2️⃣  Redirect prices.DB_PATH to a temp file in pytest’s tmp dir
    prices.DB_PATH = tmp_path / "market.sqlite"

    # 3️⃣  Refresh universe lists (live download from SSGA)
    asyncio.run(UniverseService().sync())

    # 4️⃣  Sync grouped bars from Polygon
    prices.sync_grouped_bars(verbose=True)

    # 5️⃣  Assertions against the temp DB
    with sqlite3.connect(prices.DB_PATH) as conn:
        df = pd.read_sql("SELECT * FROM daily_prices", conn)

    assert len(df) > 0, "No rows stored – check API key or rate limit"

    uni = (
        set(UniverseService().get_cohort("sp500")["symbol"])
        | set(UniverseService().get_cohort("sp400")["symbol"])
    )
    assert set(df["ticker"]).issubset(uni), "Rows contain tickers outside the universe"

    assert len(df["date"].unique()) == 1, "More than one trade_date stored"

    print(f"\n✅  {len(df):,} rows saved to {prices.DB_PATH}")
