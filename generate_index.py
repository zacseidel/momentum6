#!/usr/bin/env python
"""
Regenerate the root index.html after each run.

• Lists every report HTML found in ./reports/
• Auto-loads the most recent report into the iframe on page load
• No latest.html symlink/copy is used
"""

import re
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR   = Path(__file__).resolve().parents[0]
REPORT_DIR = BASE_DIR / "reports"
INDEX_PATH = BASE_DIR / "index.html"

# Discover report files named with a date like YYYY-MM-DD.html
PAT = re.compile(r"(\d{4}-\d{2}-\d{2})\.html$", re.I)

def get_sorted_reports():
    items = []
    for fp in REPORT_DIR.glob("*.html"):
        m = PAT.search(fp.name)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%Y-%m-%d").date()
                items.append((dt, fp.name))
            except ValueError:
                continue
    return sorted(items, reverse=True)

def generate_sidebar_links(sorted_reports):
    links = []
    for i, (dt, fname) in enumerate(sorted_reports):
        disp = dt.strftime("%B %d, %Y")
        cls  = "class='active'" if i == 0 else ""
        # Pass `this` so we can highlight the clicked link
        links.append(
            f"<a href='#' {cls} data-file='reports/{fname}' "
            f"onclick=\"loadReport('reports/{fname}', this)\">{disp}</a>"
        )
    return "\n".join(links)

def build_html(sorted_reports):
    latest_path = f"reports/{sorted_reports[0][1]}"
    sidebar_links = generate_sidebar_links(sorted_reports)
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Momentum Screener Reports</title>
  <style>
    body {{ display:flex; font-family:sans-serif; margin:0; }}
    nav  {{ width:240px; background:#f4f4f4; height:100vh; overflow-y:auto;
            padding:1em; box-shadow:2px 0 5px rgba(0,0,0,0.1); }}
    nav h2 {{ font-size:16px; margin-top:1em; }}
    nav a  {{ display:block; margin:0.5em 0; color:#333; text-decoration:none; }}
    nav a:hover {{ text-decoration:underline; }}
    nav a.active {{ font-weight:bold; }}
    main {{ flex-grow:1; padding:1em; }}
    iframe {{ width:100%; height:95vh; border:none; }}
  </style>
</head>
<body>
  <nav>
    <h2>🗂️ Weekly Reports</h2>
    {sidebar_links}
    <h2>📘 Documentation</h2>
    <a href="#" onclick="loadReport('about.html', this)">About this Site</a>
  </nav>
  <main>
    <!-- Auto-load the most recent report here -->
    <iframe id="reportFrame" src="{latest_path}"></iframe>
  </main>
  <script>
    function setActive(link) {{
      document.querySelectorAll('nav a.active').forEach(a => a.classList.remove('active'));
      if (link) link.classList.add('active');
    }}
    function loadReport(p, el) {{
      document.getElementById('reportFrame').src = p;
      setActive(el);
    }}
  </script>
</body>
</html>"""

def main():
    reports = get_sorted_reports()
    if not reports:
        print("⚠️  No reports found in", REPORT_DIR)
        sys.exit(0)

    html = build_html(reports)
    INDEX_PATH.write_text(html, encoding="utf-8")
    print(f"✅  index.html regenerated ({len(reports)} reports listed; newest auto-loaded)")

if __name__ == "__main__":
    main()
