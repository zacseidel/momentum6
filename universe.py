from __future__ import annotations

import asyncio
import io
from datetime import date
from pathlib import Path
from typing import Literal
import pandas as pd
import httpx


# --------------------------------------------------------------------------------------
# Configuration – easy to tweak later
# --------------------------------------------------------------------------------------

HOLDINGS_URLS = {
    "SPY": "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx",
    "MDY": "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-mdy.xlsx",
}


PRINT_PREFIX = "🟢"  # change this or set to "" to silence


def p(msg: str) -> None:
    """Small helper to keep prints consistent."""
    print(f"{PRINT_PREFIX} {msg}")


# --------------------------------------------------------------------------------------
# Main service
# --------------------------------------------------------------------------------------
class UniverseService:
    """Maintain latest universe CSVs & an append-only change log."""

    def __init__(self, data_dir: Path | str = "data/universe") -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    # ------------- public -------------
    async def sync(self, *, as_of: date | None = None) -> None:
        """Download, diff, and update files.  Intended to run weekly."""
        as_of = as_of or date.today()
        p("Starting universe sync …")

        sp500_df, sp400_df = await self._download_all()
        p(f"Downloaded SP500 rows: {len(sp500_df)}  |  SP400 rows: {len(sp400_df)}")

        megacap_df = self._derive_megacap(sp500_df)
        p(f"Derived megacap list (top-25 by weight, GOOG+GOOGL merged)")

        for cohort, new_df in {
            "sp500": sp500_df,
            "sp400": sp400_df,
            "megacap": megacap_df,
        }.items():
            adds, drops = self._write_and_log(cohort, new_df, as_of)
            p(f"{cohort.upper():7}  →  wrote {len(new_df):4} rows  |  +{len(adds):2} / -{len(drops):2}")

        p("Universe sync complete!  ✓")

    def get_cohort(self, cohort: Literal["megacap", "sp500", "sp400"] = "sp500") -> pd.DataFrame:
        return pd.read_csv(self.data_dir / f"{cohort}.csv")

    def get_change_log(self) -> pd.DataFrame:
        return pd.read_csv(self.data_dir / "change_log.csv")

    # ------------- internal helpers -------------
    async def _download_all(self):
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            spy_df, mdy_df = await asyncio.gather(
                self._download_holdings(client, "SPY"),
                self._download_holdings(client, "MDY"),
            )
        return spy_df, mdy_df

    async def _download_holdings(self, client: httpx.AsyncClient, ticker: str) -> pd.DataFrame:
        url = HOLDINGS_URLS[ticker]
        p(f"Fetching {ticker} holdings …")

        resp = await client.get(url)
        resp.raise_for_status()

        # SSGA file has 4 title rows → skiprows=4
        buf = io.BytesIO(resp.content)
        df = pd.read_excel(buf, engine="openpyxl", skiprows=4)

        # Strip spaces/newlines from headers
        df.columns = df.columns.str.strip()

        # ---------- find the three columns we need ----------
        def find_col(substrs):
            for col in df.columns:
                if any(s.lower() in col.lower() for s in substrs):
                    return col
            return None

        sym_col   = find_col(["Ticker", "Symbol"])
        name_col  = find_col(["Name", "Security"])
        weight_col = find_col(["Weight"])          # catches "Weight", "Weight (%)", etc.

        if None in (sym_col, name_col, weight_col):
            raise KeyError(
                f"{ticker}: couldn't locate expected headers.\n"
                f"Columns seen: {df.columns.tolist()}"
            )

        df = df[[sym_col, name_col, weight_col]].rename(
            columns={sym_col: "symbol", name_col: "name", weight_col: "weight"}
        )

        # clean & numeric weight
        df["weight"] = df["weight"].astype(str).str.rstrip("%").astype(float)

        # drop rows without a ticker symbol (trailing totals, etc.)
        df = df.dropna(subset=["symbol"]).reset_index(drop=True)
        return df



    def _derive_megacap(self, sp500: pd.DataFrame) -> pd.DataFrame:
        df = sp500.copy()
        goog_mask = df.symbol.isin(["GOOGL", "GOOG"])
        goog_weight = df.loc[goog_mask, "weight"].sum()
        df = df.loc[~goog_mask]  # drop both classes
        # add merged Alphabet entry
        df = pd.concat(
            [
                df,
                pd.DataFrame(
                    {
                        "symbol": ["GOOGL"],
                        "name": ["Alphabet Inc. (Class A & C combined)"],
                        "weight": [goog_weight],
                    }
                ),
            ],
            ignore_index=True,
        )
        return df.sort_values("weight", ascending=False).head(25).reset_index(drop=True)

    def _write_and_log(self, cohort: str, new_df: pd.DataFrame, as_of: date):
        file_path = self.data_dir / f"{cohort}.csv"
        if file_path.exists():
            old_df = pd.read_csv(file_path)
            adds = set(new_df.symbol) - set(old_df.symbol)
            drops = set(old_df.symbol) - set(new_df.symbol)
        else:
            adds, drops = set(new_df.symbol), set()

        new_df.to_csv(file_path, index=False)

        log_path = self.data_dir / "change_log.csv"
        log_path.touch(exist_ok=True)

        rows = [
            {
                "run_date": as_of.isoformat(),
                "cohort": cohort,
                "action": "added" if sym in adds else "removed",
                "symbol": sym,
            }
            for sym in (*adds, *drops)
        ]
        if rows:
            pd.DataFrame(rows).to_csv(
                log_path, mode="a", header=not log_path.stat().st_size, index=False
            )

        return adds, drops

# --- add this at the very end, right above `if __name__ == "__main__":` ---
print("DEBUG: universe.py was imported and executed")
# -------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse, sys

    print("DEBUG: argv =", sys.argv)        # <—— shows the exact CLI args

    parser = argparse.ArgumentParser(description="Sync index constituent files")
    parser.add_argument("--sync", action="store_true", help="Download and update universe files")
    args = parser.parse_args()

    print(f"DEBUG: args.sync = {args.sync}") # <—— should be True when you pass --sync

    if args.sync:
        asyncio.run(UniverseService().sync())

