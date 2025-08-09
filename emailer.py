# emailer.py
# Functions to format and send the HTML email report

import os, numbers
import pandas as pd

from jinja2 import Template
from dotenv import load_dotenv
import sqlite3
from datetime import date, datetime
from pathlib import Path
from chart_module import plot_stock_chart
import base64, io
import matplotlib
matplotlib.use("Agg")  # Use non-interactive backend for saving charts
import matplotlib.pyplot as plt



load_dotenv()

BASE_DIR = Path(__file__).resolve().parent        # repo root
DB_PATH  = BASE_DIR / "data" / "market_data.sqlite"
# optional – keep charts together
ASSETS_DIR = BASE_DIR / "assets"
CHART_DIR  = ASSETS_DIR / "charts"
CHART_DIR.mkdir(parents=True, exist_ok=True)     # en

# -- Backtracking date function -- 
from pandas.tseries.offsets import BDay


def style_return(val, darker=False):
    """
    Return a coloured <span> with sign and 1-dec %.
    If darker=True the dark palette is used.
    """

    POS_L, NEG_L = "#006400", "#c42020"     # light green / red
    POS_D, NEG_D = "#006400", "#7d0d0d"     # darker green / red

    val = as_float(val)
    if val is None:
        return "-"

    sign   = "+" if val >= 0 else "−"
    colour = (
        POS_D if (val >= 0 and darker) else
        NEG_D if (val < 0  and darker) else
        POS_L if val >= 0 else
        NEG_L
    )
    return f'<span style="color:{colour};">{sign}{abs(val):.1%}</span>'

def as_float(x):
    """
    Convert '484.1%' -> 4.841, '0.27' -> 0.27, return None on failure/NaN.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, str):
        x = x.strip()
        had_percent = x.endswith("%")
        if had_percent:
            x = x.rstrip("%").strip()
        try:
            num = float(x)
            return num / 100 if had_percent else num
        except ValueError:
            return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def backtrack_to_available_date(conn, ticker, date_str, max_days=7):
    """Backtrack from date_str to find most recent trading day with data."""
    d = pd.to_datetime(date_str)
    for _ in range(max_days):
        ds = d.strftime("%Y-%m-%d")
        exists = conn.execute(
            "SELECT 1 FROM daily_prices WHERE ticker = ? AND date = ?",
            (ticker, ds)
        ).fetchone()
        if exists:
            return ds
        d -= BDay(1)
    return None

def get_price_backtracked(conn, ticker, anchor_date, max_days=7):
    """Float closing price on last trading day ≤ anchor_date, or None."""
    ds = backtrack_to_available_date(conn, ticker, anchor_date, max_days)
    if ds:
        row = conn.execute(
            "SELECT close FROM daily_prices WHERE ticker=? AND date=?",
            (ticker, ds)
        ).fetchone()
        try:
            return float(row[0]) if row else None    # ← cast right here
        except (TypeError, ValueError):
            return None
    return None

def is_real(x):
    return isinstance(x, numbers.Real) and not pd.isna(x)

# ─── 1.  rename + broaden the public formatter  ──────────────────────
def format_html_report(top10_mega_df, top10_spy_df, top10_mdy_df, report_date=None):
    """
    Build full HTML with THREE cohorts: Megacap • SP500 • SP400.
    """
        # ---- Order MEGA by weight from universe/megacap.csv ----
    mega_sorted = top10_mega_df.copy()

    try:
        weights_path = BASE_DIR / "data" / "universe" / "megacap.csv"
        w = pd.read_csv(weights_path)

        # Normalize column names for a clean merge
        w = w.rename(columns={"symbol": "ticker"})
        w["ticker"] = w["ticker"].str.upper()

        # Ensure MEGA df has ticker col for merge
        if "ticker" not in mega_sorted.columns and "symbol" in mega_sorted.columns:
            mega_sorted = mega_sorted.rename(columns={"symbol": "ticker"})
        mega_sorted["ticker"] = mega_sorted["ticker"].str.upper()

        # Merge in weight
        mega_sorted = mega_sorted.merge(
            w[["ticker", "weight"]], on="ticker", how="left", validate="m:1"
        )
        mega_sorted["weight"] = pd.to_numeric(mega_sorted["weight"], errors="coerce")

        # --- New: split into top 5 and next 5, sort each by weight ---
        top5 = mega_sorted.iloc[:5].sort_values(["weight", "ticker"], ascending=[False, True])
        next5 = mega_sorted.iloc[5:10].sort_values(["weight", "ticker"], ascending=[False, True])

        # Combine back
        mega_sorted = pd.concat([top5, next5], ignore_index=True)

    except Exception as e:
        print(f"⚠️ Could not apply megacap weight ordering, using existing order: {e}")


    mega_summary, mega_cards = _build_index_sections(mega_sorted, "MEGA", report_date)
    spy_summary,  spy_cards  = _build_index_sections(top10_spy_df,  "SPY",  report_date)
    mdy_summary,  mdy_cards  = _build_index_sections(top10_mdy_df,  "MDY",  report_date)

    human_date = pd.to_datetime(report_date or pd.Timestamp.today()).strftime("%B %d, %Y")

    page_tpl = Template(r"""
    <html>
      <head><meta charset="utf-8"><title>Momentum Report – {{ date }}</title></head>
      <body style="font-family:Arial,Helvetica,sans-serif;">
        <h2>📈 Momentum Report – {{ date }}</h2>

        <!-- ===== SUMMARY ===== -->
        <h3>Megacap Stocks - Top 25 stocks by Market Cap in the SP500</h3> {{ mega_summary | safe }}
        <h3>S&P 500 - Large Cap Stocks</h3>  {{ spy_summary  | safe }}
        <h3>S&P 400 - MidCap Stocks</h3>  {{ mdy_summary  | safe }}

        <!-- ===== ENRICHED ===== -->
        <h2>Detail for each Company</h2>
        <h3>— MegaCap Stocks —</h3> {{ mega_cards | safe }}
        <h3>— S&P 500 —</h3>  {{ spy_cards  | safe }}
        <h3>— S&P 400 —</h3>  {{ mdy_cards  | safe }}
      </body>
    </html>
    """)
    return page_tpl.render(
        date=human_date,
        mega_summary=mega_summary, mega_cards=mega_cards,
        spy_summary =spy_summary,  spy_cards =spy_cards,
        mdy_summary =mdy_summary,  mdy_cards =mdy_cards,
    )
# ---------------------------------------------------------------------------
def _build_index_sections(top10_df, index_label="SPY", report_date=None):
    """
    Parameters
    ----------
    top10_df    : DataFrame  – the 10‑row result set for one index
    index_label : str        – "SPY" or "MDY" (for logging / headings)
    report_date : same as before

    Returns
    -------
    summary_html : str   – summary block with benchmark + change list
    cards_html   : str   – concatenated <div> cards for the 10 stocks
    """
    # ------------------------------------------------------------------
    # 🔸🔸  BEGIN: this is your EXISTING format_html_email logic,
    #            **from the first “if report_date is None:” line
    #            down to, but NOT including, the Template(...) at
    #            the very bottom**.
    #
    #     • Keep every helper (style_return, as_float, backtrack_to_available_date,
    #       get_price_backtracked, plot_stock_chart) unchanged.
    #     • At the point where the old function created `summary_html`
    #       and built the list `enriched`, do this instead:
    #
    # ------------------------------------------------------------------
    # (after building `summary_html` and the `enriched` list)

        # Normalise report_date → pandas.Timestamp so we can always call
    # .date(), .strftime(), do DateOffset maths, etc.
    # ------------------------------------------------------------------
    if report_date is None:
        report_ts = pd.Timestamp.today().normalize() - pd.DateOffset(days=1)
    elif isinstance(report_date, str):
        report_ts = pd.to_datetime(report_date).normalize()
    elif isinstance(report_date, datetime):
        report_ts = pd.Timestamp(report_date).normalize()
    elif isinstance(report_date, date):            # <-- handles datetime.date
        report_ts = pd.Timestamp(report_date).normalize()
    else:  # already some Timestamp-like
        report_ts = pd.Timestamp(report_date).normalize()

    price_ts = report_ts - pd.DateOffset(days=1)

    formatted_report_date = report_ts.strftime("%B %d, %Y")
    current_report_date_str = report_ts.date().isoformat()

    formatted_price_date = price_ts.strftime("%B %d, %Y")
    current_price_date_str = price_ts.date().isoformat()

    print(top10_df[["ticker","current_return","last_week_return"]].head())
    print([as_float(x) for x in top10_df["last_week_return"].head()])

    tickers = [t.strip().upper() for t in top10_df["ticker"].tolist()]
    current_tickers = set(tickers)


    # --- Fetch from DB ---
    with sqlite3.connect(DB_PATH) as conn:
        # 1. Resolve most recent prior report date
                # ------------------------------------------------------------------
        # 1. Resolve the most‑recent prior report date for *this* index
        # ------------------------------------------------------------------
        table_map = {"MEGA": "top10_mega", "SPY": "top10_spy", "MDY": "top10_mdy"}
        history_table = table_map.get(index_label.upper())   # None → no history

        if history_table is None:
            prev_tickers = set()          # e.g. future Watchlist block
            prior_date_str = None
        else:
            # most recent run *before* today
            try:
                prior_date_row = pd.read_sql(
                    f"""
                    SELECT DISTINCT date
                    FROM {history_table}
                    WHERE date < ?
                    ORDER BY date DESC
                    LIMIT 1
                    """,
                    conn, params=[current_report_date_str]
                )
            except sqlite3.Error as err:          # table missing?
                print(f"⚠️ {history_table} lookup failed: {err}")
                prior_date_row = pd.DataFrame()

            if not prior_date_row.empty:
                prior_date_str = prior_date_row["date"].iloc[0]
                prev = pd.read_sql(
                    f"SELECT DISTINCT ticker FROM {history_table} WHERE date = ?",
                    conn, params=[prior_date_str]
                )
                prev_tickers = set(prev["ticker"].str.strip().str.upper())
            else:
                prior_date_str = None
                prev_tickers = set()


        # 2. Fetch VOO prices

        # Backtrack VOO dates to available trading days
        voo_dates = {
            "current": backtrack_to_available_date(conn, "VOO", current_report_date_str),
            "one_year_ago": backtrack_to_available_date(
                conn, "VOO", (report_ts - pd.DateOffset(years=1)).strftime("%Y-%m-%d")
            ),
            "one_week_ago": backtrack_to_available_date(
                conn, "VOO",
                (report_ts   - pd.DateOffset(weeks=1)).strftime("%Y-%m-%d"))
        }
        print("📅 Resolved VOO dates:", voo_dates)


        voo = pd.read_sql(
            "SELECT date, close FROM daily_prices WHERE ticker = 'VOO' AND date IN (?, ?, ?)",
            conn,
            params=[voo_dates["current"], voo_dates["one_year_ago"], voo_dates["one_week_ago"]]
        ).set_index("date")["close"]
        voo.index = voo.index.astype(str)
        print("📈 Retrieved VOO prices:", voo.to_dict())


        # 3. Fetch metadata + news
        meta = pd.read_sql(
            f"SELECT ticker, name, description FROM company_metadata WHERE ticker IN ({','.join(['?']*len(tickers))})",
            conn, params=tickers
        )
        news = pd.read_sql(
            f"SELECT ticker, headline, url, published_utc FROM company_news WHERE ticker IN ({','.join(['?']*len(tickers))}) ORDER BY published_utc DESC",
            conn, params=tickers
        )

        # 4. Fetch prices for summary
        all_compare = list(current_tickers.union(prev_tickers))
        print("🔎 All compare tickers:", all_compare)

        price_rows = pd.read_sql(
            f"SELECT ticker, date, close FROM daily_prices WHERE date = ? AND ticker IN ({','.join(['?']*len(all_compare))})",
            conn, params=[current_price_date_str] + all_compare
        )
        # extra dates for dropped-ticker return calc
        price_curr = {
            tk: get_price_backtracked(conn, tk, current_price_date_str, max_days=7)
                for tk in all_compare
            }

        # --------------------------------------------------------------------
        one_week_anchor = (report_ts - pd.DateOffset(weeks=1)).strftime("%Y-%m-%d")
        one_year_anchor = (report_ts - pd.DateOffset(years=1)).strftime("%Y-%m-%d")

        price_wk = {tk: get_price_backtracked(conn, tk, one_week_anchor) for tk in all_compare}
        price_yr = {tk: get_price_backtracked(conn, tk, one_year_anchor) for tk in all_compare}

        prices_raw = price_rows.set_index(price_rows["ticker"].str.upper())["close"].to_dict()
        prices = {k: float(v) for k, v in prices_raw.items()}
# -------------------------------------------------------------------

        print("🔎 Available price tickers:", list(prices.keys()))
        print("🔎 Tickers in report:", tickers)

    # --- Compute VOO benchmark ---
    if all(date in voo for date in voo_dates.values()):
        voo_now = voo[voo_dates["current"]]
        voo_then = voo[voo_dates["one_year_ago"]]
        voo_week_ago = voo[voo_dates["one_week_ago"]]
        voo_ret_12m = (voo_now / voo_then - 1) if voo_then > 0 else None
        voo_ret_1w = (voo_now / voo_week_ago - 1) if is_real(voo_week_ago) else None
        voo_price_fmt = f"${voo_now:.2f}" if is_real(voo_now) else "—"        
        voo_12m_span  = style_return(voo_ret_12m)        # light palette
        voo_1w_span   = style_return(voo_ret_1w)         # light palette

        voo_line = f"<p><strong>Benchmark (VOO):</strong> {voo_price_fmt} ({voo_12m_span} last 12M, {voo_1w_span} last week)</p>"
    else:
        voo_line = "<p><strong>Benchmark (VOO):</strong> Not available</p>"

    # --- Merge everything ---
    meta_dict = meta.set_index("ticker").to_dict("index")
    news_grouped = news.groupby("ticker")

    enriched = []
    print("🔎 Prices dict keys:", list(prices.keys())[:10])

    summary_id = f"summary-{index_label}"      # NEW


    for _, row in top10_df.iterrows():
        ticker = row["ticker"].strip().upper()

        # BEFORE you append to `enriched`  (just after headlines = …)

        # ------------------------------------------------------------
        # build / embed price chart
  # -----------------------------------------------------------------
        #  generate chart  →  base-64  (fail-safe)
        try:
            fig, _ = plot_stock_chart(ticker, save_path=None)
            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
            plt.close(fig)                         # free memory
            buf.seek(0)
            chart_b64 = base64.b64encode(buf.read()).decode()
            chart_uri = f"data:image/png;base64,{chart_b64}"
        except Exception as err:
            print(f"⚠️  chart for {ticker} skipped: {err}")
            chart_uri = ""                         # empty → no <img>
# -----------------------------------------------------------------

        # ------------------------------------------------------------
        
        # darker flag in loops       
        
        last_week_val = as_float(row["last_week_return"])
        
        darker = (
            as_float(voo_ret_1w) is not None and
            last_week_val is not None and
            abs(last_week_val) > abs(voo_ret_1w)
        )
        price_val = prices.get(ticker)
        try:
            price_fmt = f"{float(price_val):.2f}"
        except:
            price_fmt = "—"

        company = meta_dict.get(ticker, {})
        headlines = news_grouped.get_group(ticker).to_dict("records") if ticker in news_grouped.groups else []
        anchor_id = f"{index_label}-{ticker}"


        enriched.append({
            "ticker": ticker,
            "price": price_fmt,
            "current_return": row["current_return"],
            "last_month_return": row["last_month_return"],
            "last_week_span": style_return(last_week_val, darker=darker),
            "chart_uri": chart_uri,
            "rank_change": row["rank_change"],
            "name": company.get("name", ""),
            "description": company.get("description", ""),
            "headlines": headlines[:5],
            "anchor_id": anchor_id,
            "summary_id": summary_id,
            "index_label": index_label,
        })

    # --- Build Summary ---
    added = current_tickers - prev_tickers
    dropped = prev_tickers - current_tickers
    continuing = current_tickers & prev_tickers

    summary_lines = []

    for ticker in tickers:                      # keep original order
        row         = top10_df.loc[top10_df["ticker"].str.upper() == ticker].iloc[0]
        ret12_num   = as_float(row["current_return"])
        retwk_num   = as_float(row["last_week_return"])
        price_val   = prices.get(ticker, "—")

        # price formatting ------------------------------------------------------
        if is_real(price_val):
            price_fmt = f"${price_val:.2f}"
        else:
            price_fmt = f"${price_val}"

        # darker?  |stock_1w| > |VOO_1w|  (only if both numeric) ---------------
        darker = (
            as_float(voo_ret_1w) is not None and
            retwk_num is not None and
            retwk_num > voo_ret_1w
        )

        # build coloured spans --------------------------------------------------
        retwk_span = style_return(retwk_num, darker=darker)
        ret12_span = style_return(ret12_num)          # 12-month always light
        anchor_id   = f"{index_label}-{ticker}"           # exists in cards
        ticker_link = f'<a href="#{anchor_id}" style="color:inherit; text-decoration:none;">{ticker}</a>'

        text = f"{ticker_link} – {price_fmt} ({ret12_span} last 12M, {retwk_span} last week)"

        if ticker in added:
            summary_lines.append(f"<i><span style=\"color:#0000FF\">{text}</span></i>")
        elif ticker in continuing:
            summary_lines.append(text)

        if index_label == "MEGA" and ticker==tickers[4]:
            summary_lines.append("<br>")

    for ticker in sorted(dropped):
        price_now = price_curr.get(ticker)
        price_wk0 = as_float(price_wk.get(ticker))
        price_yr0 = as_float(price_yr.get(ticker))

        price_fmt = f"${price_now:.2f}" if is_real(price_now) else f"${price_now}"
        retwk_num = (price_now / price_wk0 - 1) if is_real(price_wk0) else None
        ret12_num = (price_now / price_yr0 - 1) if is_real(price_yr0) else None

        retwk_span = style_return(retwk_num)     # dropped stocks: always light colours
        ret12_span = style_return(ret12_num)

        text = f"{ticker} – {price_fmt} ({ret12_span} last 12 M, {retwk_span} last week)"
        summary_lines.append(f"<span style=\"color:#808080\">{text}</span>")

    summary_html = voo_line + f"<br>".join(summary_lines) + "</p><p>New stocks in <i><span style=\"color:#0000FF\">blue italic</span></i>, dropped stocks in <span style=\"color:#808080\">gray</span></p>"

    # ---------- build detailed card markup ----------------------------
    card_tpl = Template("""
        <div id="{{ anchor_id }}" style="margin-bottom:30px; padding:10px; border-bottom:1px solid #ccc;">
            <h3>{{ stock.ticker }} – {{ stock.name }} – ${{ stock.price }}</h3>

            {% if stock.chart_uri %}
            <img src="{{ stock.chart_uri }}" alt="Price chart for {{ stock.ticker }}"
                style="max-width: 100%; height: auto; margin-bottom: 10px;">
            {% endif %}

            <p><strong>Current 12M Return:</strong> {{ stock.current_return }}
            | <strong>12M Return, as of Last Month:</strong> {{ stock.last_month_return }}
            | <strong>Rank Change:</strong> {{ stock.rank_change }}
            | <strong>Last Week Return:</strong> {{ stock.last_week_span | safe }}
            </p>
            <p>{{ stock.description }}</p>
            <ul>
                {% for item in stock.headlines %}
                    <li><a href="{{ item.url }}" target="_blank">{{ item.headline }}</a>
                        <em>({{ item.published_utc[:10] }})</em></li>
                {% endfor %}
            </ul>

            <!-- back‑link -->
            <p style="margin-top:8px;">
                <a href="#{{ summary_id }}">⬆︎ back to {{ index_label }} summary</a>
            </p>
        </div>
    """)

    cards_html = "\n".join(card_tpl.render(stock = s, **s) for s in enriched)

    # ------------------------------------------------------------------
    # 🔸🔸  END of copied logic
    # ------------------------------------------------------------------

    return summary_html, cards_html


