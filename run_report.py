#!/usr/bin/env python
# run_report.py
# Orchestrates the momentum screener workflow and sends the weekly report.
# Uses the exact same logic as generate_report.py, plus email delivery.

import os
import asyncio
from datetime import date, datetime
from pathlib import Path
from dotenv import load_dotenv


# --- repo imports -----------------------------------------------------------
from universe import UniverseService
from prices   import sync_grouped_bars, get_target_dates
from ranking  import (
    get_price_snapshots, compute_returns_and_ranks,
    store_top10_mega_picks, store_top10_picks, store_top10_mdy_picks,
)
from report   import cache_company_data
from emailer  import format_html_report


load_dotenv()

# ---------------------------------------------------------------------------
REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
def _resolve_target_dates(anchor: date) -> dict:
    """
    For the standard set of lookup dates, resolve the actual trading dates
    (backtracking as needed) and return a label->resolved_date map.
    """
    raw_targets = get_target_dates(anchor)
    resolved_by_requested = {}
    resolved_targets = {}

    # Resolve each unique requested date by actually syncing grouped bars.
    # This both backfills/caches data and returns the *resolved* trading day.
    for requested_iso in set(raw_targets.values()):
        resolved_dt = sync_grouped_bars(as_of=date.fromisoformat(requested_iso), verbose=False)
        resolved_by_requested[requested_iso] = resolved_dt.isoformat()

    # Map labels to resolved dates
    for label, requested_iso in raw_targets.items():
        resolved_targets[label] = resolved_by_requested[requested_iso]

    print("📅 Date resolution map:")
    for label in raw_targets:
        print(f".  {label}: requested {raw_targets[label]} → resolved {resolved_targets[label]} ")

    return resolved_targets


def build_report(anchor: date) -> str:
    # 1️⃣ Universe sync for the given anchor date
    asyncio.run( UniverseService().sync(as_of=anchor) )

    # 2️⃣ Resolve target dates and ensure OHLCV cached for each resolved day
    targets = _resolve_target_dates(anchor)
    for ds in set(targets.values()):
        sync_grouped_bars(as_of=date.fromisoformat(ds), verbose=False)  # idempotent, safe

    # 3️⃣ Rankings for all three cohorts
    mega_df, mega_dates = get_price_snapshots(targets, index_type="megacap")
    spy_df , spy_dates  = get_price_snapshots(targets, index_type="sp500")
    mdy_df , mdy_dates  = get_price_snapshots(targets, index_type="sp400")

    mega_ranks = compute_returns_and_ranks(mega_df, mega_dates)
    spy_ranks  = compute_returns_and_ranks(spy_df , spy_dates)
    mdy_ranks  = compute_returns_and_ranks(mdy_df , mdy_dates)

    top10_mega = store_top10_mega_picks(mega_ranks, run_date=anchor)
    top10_spy  = store_top10_picks     (spy_ranks , run_date=anchor)
    top10_mdy  = store_top10_mdy_picks (mdy_ranks , run_date=anchor)

    # 4️⃣ Cache news/metadata for all displayed tickers
    cache_company_data(
        top10_mega["ticker"].tolist()
        + top10_spy["ticker"].tolist()
        + top10_mdy["ticker"].tolist()
    )
    print("✅  Cached metadata and news for all tickers.")

    # 5️⃣ Compose HTML
    html = format_html_report(top10_mega, top10_spy, top10_mdy, report_date=anchor)
    return html


def main():
    # Accept optional CLI override like: python run_report.py 2025-07-25
    import sys
    if len(sys.argv) > 1:
        anchor = datetime.fromisoformat(sys.argv[1]).date()
    else:
        anchor = date.today()

    print(f"🚀  Momentum Screener Pipeline for {anchor} …")

    # Build the report (same as generate_report)
    html = build_report(anchor)

    # Save HTML copy
    outfile = REPORT_DIR / f"momentum_{anchor.isoformat()}.html"
    outfile.write_text(html, encoding="utf-8")
    print(f"📝  Saved HTML → {outfile}")

    # Update the index page (show latest automatically)
    import generate_index
    generate_index.main()
    print("📄  Index updated with new report link.")


if __name__ == "__main__":
    main()
