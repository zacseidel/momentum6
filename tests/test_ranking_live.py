# tests/test_ranking_live.py
import os, sqlite3, asyncio
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
from dotenv import load_dotenv

import prices, ranking
from universe import UniverseService
from init_db import initialize_database        # ← to create tables

load_dotenv()

TODAY = date.today()
BASE_FRIDAY = TODAY - timedelta(days=(TODAY.weekday() + 3) % 7)  # last Friday

@pytest.mark.skipif(
    not os.getenv("POLYGON_API_KEY"),
    reason="POLYGON_API_KEY missing – skipping live test",
)
def test_live_ranking_pipeline(tmp_path):
    db_file: Path = tmp_path / "market_live.sqlite"
    prices.DB_PATH  = db_file
    ranking.DB_PATH = db_file

    # ► create lean schema in the temp DB
    with sqlite3.connect(db_file) as conn:
        schema_top10 = """
            ticker TEXT,
            date   DATE,
            current_return    TEXT,
            last_month_return TEXT,
            last_week_return  TEXT,
            current_rank      REAL,
            last_month_rank   REAL,
            rank_change       REAL,
            PRIMARY KEY (ticker,date)
        """
        conn.execute("CREATE TABLE daily_prices (ticker TEXT, date DATE, open REAL, high REAL, low REAL, close REAL, volume INTEGER, PRIMARY KEY (ticker,date))")
        conn.execute(f"CREATE TABLE top10_picks ({schema_top10})")
        conn.execute(f"CREATE TABLE top10_mdy  ({schema_top10})")
        conn.execute(f"CREATE TABLE top10_mega ({schema_top10})")                    # create top10_*** tables

    # 1. universe lists
    asyncio.run(UniverseService().sync())

    # 2. anchor dates
    anchors  = prices.get_target_dates(BASE_FRIDAY + timedelta(days=1))
    run_date = anchors["yesterday"]

    # 3. pull prices for each anchor day
    for ds in set(anchors.values()):
        prices.sync_grouped_bars(as_of=date.fromisoformat(ds), verbose=False)

    # 4. ranking for each cohort
    store_map = {
        "megacap": ranking.store_top10_mega_picks,
        "sp500":   ranking.store_top10_picks,
        "sp400":   ranking.store_top10_mdy_picks,
    }

    for cohort, store_fn in store_map.items():
        df, resolved = ranking.get_price_snapshots(
            anchors, index_type=cohort, db_path=db_file
        )
        assert not df.empty, f"{cohort}: snapshot empty"

        ranks = ranking.compute_returns_and_ranks(df, resolved)
        top10 = store_fn(ranks, run_date=run_date, db_path=db_file)

        assert 0 < len(top10) <= 10
        assert set(top10["date"]) == {run_date}

    # 5. final check
    with sqlite3.connect(db_file) as conn:
        for tbl in ("top10_mega", "top10_picks", "top10_mdy"):
            assert conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0] > 0
