# init_db.py – minimal schema
import sqlite3, pathlib
DB_PATH = pathlib.Path("data/market_data.sqlite")
DB_PATH.parent.mkdir(exist_ok=True)

def initialize_database():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_prices (
            ticker TEXT,
            date   DATE,
            open   REAL,
            high   REAL,
            low    REAL,
            close  REAL,
            volume INTEGER,
            PRIMARY KEY (ticker,date)
        )""")

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
        cur.execute(f"CREATE TABLE IF NOT EXISTS top10_spy ({schema_top10})")
        cur.execute(f"CREATE TABLE IF NOT EXISTS top10_mdy  ({schema_top10})")
        cur.execute(f"CREATE TABLE IF NOT EXISTS top10_mega ({schema_top10})")

        conn.commit()
        print(f"✅ Database initialized at {DB_PATH}")

if __name__ == "__main__":
    initialize_database()