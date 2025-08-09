#!/usr/bin/env python
"""
Generate a momentum report for any Friday in the past and save
the HTML to reports/YYYY-MM-DD.html.  Uses the same pipeline
as run_report.py but skips e-mail.

Examples
--------
python generate_report.py               # today’s report
python generate_report.py 2025-07-25    # historic Friday
"""

import sys, pathlib, os
from datetime import date, datetime
import asyncio
import pandas as pd

# --- repo imports -----------------------------------------------------------
from universe import UniverseService
from prices   import sync_grouped_bars, get_target_dates     # <- NEW import
from ranking  import (
    get_price_snapshots, compute_returns_and_ranks,
    store_top10_mega_picks, store_top10_picks, store_top10_mdy_picks,
)
from report   import cache_company_data
from emailer  import format_html_report   # new triple-section formatter
# --- top of generate_report.py -------------------------------------

# ---------------------------------------------------------------------------
REPORT_DIR = pathlib.Path("reports")
REPORT_DIR.mkdir(exist_ok=True)

# -------------------------------------------------------------------

def _resolve_target_dates(anchor: date) -> dict:
    """
    For the standard setof lookup dates, resolve the target dates
    """
    raw_targets = get_target_dates(anchor)
    resolved_by_requested = {}
    resolved_targets = {}

    for requested_iso in set(raw_targets.values()):
        resolved_dt = sync_grouped_bars(as_of=date.fromisoformat(requested_iso), verbose=False)
        resolved_by_requested[requested_iso] = resolved_dt.isoformat()

    for label, requested_iso in raw_targets.items():
        resolved_targets[label] = resolved_by_requested[requested_iso]
    
    print("📅 Date resolution map:")
    for label in raw_targets:
        print(f".  {label}: requested {raw_targets[label]} → resolved {resolved_targets[label]} ")

    return resolved_targets


def build_report(anchor: date) -> dict:
    asyncio.run( UniverseService().sync(as_of=anchor) )

    # 2️⃣ pull prices for all anchor days
    targets = _resolve_target_dates(anchor)
    for ds in set(targets.values()):
        sync_grouped_bars(as_of=date.fromisoformat(ds), verbose=False)   # <- NEW

    # --- 2. rankings for all three cohorts --------------------------------
    mega_df, mega_dates = get_price_snapshots(targets, index_type="megacap")
    spy_df , spy_dates  = get_price_snapshots(targets, index_type="sp500")
    mdy_df , mdy_dates  = get_price_snapshots(targets, index_type="sp400")

    mega_ranks = compute_returns_and_ranks(mega_df, mega_dates)
    spy_ranks  = compute_returns_and_ranks(spy_df , spy_dates)
    mdy_ranks  = compute_returns_and_ranks(mdy_df , mdy_dates)

    top10_mega = store_top10_mega_picks(mega_ranks, run_date=anchor)
    top10_spy  = store_top10_picks     (spy_ranks , run_date=anchor)
    top10_mdy  = store_top10_mdy_picks (mdy_ranks , run_date=anchor)

    # --- 3. news + metadata cache ----------------------------------------
    cache_company_data(
        top10_mega["ticker"].tolist()
        + top10_spy["ticker"].tolist()
        + top10_mdy["ticker"].tolist()
    )

    # --- 4. compose HTML --------------------------------------------------
    html = format_html_report(top10_mega, top10_spy, top10_mdy, report_date=anchor)
    return html


def main():
    if len(sys.argv) > 1:
        anchor = datetime.fromisoformat(sys.argv[1]).date()
    else:
        anchor = date.today()

    outfile = REPORT_DIR / f"momentum_{anchor.isoformat()}.html"
    print(f"⏳  Building report for {anchor} …")

    html = build_report(anchor)
    outfile.write_text(html, encoding="utf-8")
    print(f"✅  Report written → {outfile}")
    import generate_index
    generate_index.main()  # update index.html with new report link
    print("📄  Index updated with new report link.")

if __name__ == "__main__":
    main()
