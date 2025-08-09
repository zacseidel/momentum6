# tests/test_ranking_live.py
"""
Live end-to-end test for the momentum pipeline.

* Uses real SSGA & Polygon calls.
* Writes to a temp SQLite DB under pytest's tmp_path.
* Skips automatically if POLYGON_API_KEY is not set.

Adjust TEST_BASE_DATE below (YYYY-MM-DD) to run the test for any *past* week.
"""

import os, sqlite3, asyncio
from datetime import date, timedelta, datetime as dt
from pathlib import Path

import pandas as pd
import pytest
from dotenv import load_dotenv

import prices, ranking
from universe import UniverseService

load_dotenv()                              # POLYGON_API_KEY from .env




# ── 1. choose an anchor week (default = last Friday before today) ───────────
TODAY          = date.today()
TEST_BASE_DATE = TODAY - timedelta(days=(TODAY.weekday() + 3) % 7)   # last Friday
# e.g. set TEST_BASE_DATE = date(2025, 8, 1) to hard-code

@pytest.mark.skipif(
    not os.getenv("POLYGON_API_KEY"),
    reason="POLYGON_API_KEY not configured – skipping live ranking test",
)
def test_live_ranking_pipeline(tmp_path):
    # ── 2. temp DB for both modules ─────────────────────────────────────────
    db_file: Path = tmp_path / "market_live.sqlite"
    prices.DB_PATH  = db_file
    ranking.DB_PATH = db_file

    # ── 3. refresh universe lists (SSGA XLSX) ──────────────────────────────
    asyncio.run(UniverseService().sync())

    # ── 4. pull grouped bars for the week ending TEST_BASE_DATE ────────────
    prices.sync_grouped_bars(as_of=TEST_BASE_DATE, verbose=True)

    # ── 5. build anchor dates *relative to the run week* -------------------
    anchors = prices.get_target_dates(TEST_BASE_DATE + timedelta(days=1))
    run_date = anchors["yesterday"]          # should equal TEST_BASE_DATE

    # ── 6. snapshot → ranking → DB store for each cohort -------------------
    store_map = {
        "megacap": ranking.store_top10_mega_picks,
        "sp500":   ranking.store_top10_picks,
        "sp400":   ranking.store_top10_mdy_picks,
    }

    for cohort, store_fn in store_map.items():
        df, resolved = ranking.get_price_snapshots(
            anchors, index_type=cohort, db_path=db_file
        )
        assert not df.empty, f"{cohort}: no snapshot data"

        ranks = ranking.compute_returns_and_ranks(df, resolved)
        top10 = store_fn(ranks, run_date=run_date, db_path=db_file)

        assert 0 < len(top10) <= 10
        assert set(top10["date"]) == {run_date}

    # ── 7. verify tables exist ---------------------------------------------
    with sqlite3.connect(db_file) as conn:
        for tbl in ("top10_mega", "top10_picks", "top10_mdy"):
            rows = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            assert rows > 0, f"{tbl} is empty"

    print(f"\n✅  Stored top-10 tables for week ending {run_date} → {db_file}")
