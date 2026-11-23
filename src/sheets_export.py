# src/sheets_export.py
"""
Copies small, dashboard-sized tables from BigQuery marts into a Google Sheet.
Tableau Public refreshes workbooks connected to Google Sheets on its own,
so this is the last step that keeps the public dashboard current.

Sheet limit is 10 million cells. The tables below total well under 1 million.
"""
import logging
import os
import time
from urllib.parse import quote
from datetime import date, datetime

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CHUNK_ROWS = 20_000

# tab name -> query. Column names match the old Snowflake marts where possible,
# so the existing Tableau workbook can be re-pointed with little rework.
EXPORTS = {
    "fact_retail_prices": """
        SELECT period, period_date, state_id, state_name, sector_id, sector_name,
               price_cents_kwh, sales_mwh, customers,
               rolling_avg_12m, rolling_std_12m, z_score, is_anomaly
        FROM `{{project}}.eia_marts.fact_retail_prices`
        ORDER BY state_id, period""",
    # Collapsed from ~1.3M fuel-code rows to one row per state, month and fuel category.
    "fact_generation": """
        SELECT period, period_date, state_id, fuel_category,
               SUM(generation_mwh)   AS generation_mwh,
               SUM(generation_share) AS generation_share
        FROM `{{project}}.eia_marts.fact_generation`
        GROUP BY period, period_date, state_id, fuel_category
        ORDER BY state_id, period, fuel_category""",
    "dim_state": "SELECT * FROM `{{project}}.eia_marts.dim_state` ORDER BY state_id",
    "fact_rto_heatmap": """
        SELECT month, hour_of_day, region_id, region_name, avg_demand_mwh
        FROM `{{project}}.eia_marts.fact_rto_heatmap`
        ORDER BY month, region_id, hour_of_day""",
    "fact_rto_regional": """
        SELECT month, region_id, region_name, avg_demand_mwh, peak_demand_mwh
        FROM `{{project}}.eia_marts.fact_rto_regional`
        ORDER BY month, region_id""",
    "fact_anomaly_leaderboard": "SELECT * FROM `{{project}}.eia_marts.fact_anomaly_leaderboard`",
}


# Columns that must stay text. Sheets would otherwise turn "90" into a number and "2020-01"
# into a date, and Tableau then reads the column as numeric and blanks out values like "TX".
# A leading apostrophe forces text and is not stored in the cell.
TEXT_COLUMNS = {"period", "state_id", "sector_id", "region_id"}


def format_cell(v):
    """Turn a BigQuery value into something the Sheets API accepts."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, float):
        return round(v, 6)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


API = "https://sheets.googleapis.com/v4/spreadsheets"


def _service():
    """An authenticated requests session. Same auth path BigQuery uses."""
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    creds, _ = google.auth.default(scopes=SCOPES)
    return AuthorizedSession(creds)


def _call(session, method: str, url: str, **kw):
    """Call the Sheets REST API, retrying on rate limits. Raises with Google's message."""
    for attempt in range(5):
        r = session.request(method, url, timeout=120, **kw)
        if r.status_code in (429, 500, 502, 503):
            time.sleep(10 * (attempt + 1))
            continue
        if not r.ok:
            raise RuntimeError(f"Sheets API {r.status_code}: {r.text[:400]}")
        return r.json() if r.text else {}
    raise RuntimeError(f"Sheets API still failing after retries: {r.status_code} {r.text[:200]}")


def _ensure_tab(session, sheet_id: str, title: str, rows: int, cols: int) -> None:
    """Create the tab if needed and size its grid so the write fits."""
    meta = _call(session, "GET", f"{API}/{sheet_id}", params={"fields": "sheets.properties"})
    existing = {s["properties"]["title"]: s["properties"] for s in meta.get("sheets", [])}
    if title not in existing:
        req = {"addSheet": {"properties": {"title": title,
                                           "gridProperties": {"rowCount": rows, "columnCount": cols}}}}
    else:
        req = {"updateSheetProperties": {
            "properties": {"sheetId": existing[title]["sheetId"],
                           "gridProperties": {"rowCount": rows, "columnCount": cols}},
            "fields": "gridProperties(rowCount,columnCount)"}}
    _call(session, "POST", f"{API}/{sheet_id}:batchUpdate", json={"requests": [req]})


def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def write_tab(session, sheet_id: str, title: str, header: list, rows: list) -> None:
    total_rows = max(len(rows) + 1, 2)
    _ensure_tab(session, sheet_id, title, total_rows, len(header))
    _call(session, "POST", f"{API}/{sheet_id}/values/{quote(chr(39) + title + chr(39), safe='')}:clear", json={})
    last_col = _col_letter(len(header))
    data = [header] + rows
    for start in range(0, len(data), CHUNK_ROWS):
        chunk = data[start:start + CHUNK_ROWS]
        rng = f"'{title}'!A{start + 1}:{last_col}{start + len(chunk)}"
        _call(session, "PUT", f"{API}/{sheet_id}/values/{quote(rng, safe='')}",
              params={"valueInputOption": "USER_ENTERED"}, json={"values": chunk})


def row_count(session, sheet_id: str, title: str) -> int:
    rng = quote(f"'{title}'!A:A", safe="")
    vals = _call(session, "GET", f"{API}/{sheet_id}/values/{rng}").get("values", [])
    return max(len(vals) - 1, 0)


def publish(bq, sheet_id: str | None = None) -> dict:
    sheet_id = sheet_id or os.environ["SHEET_ID"]
    session = _service()
    counts = {}
    for title, sql in EXPORTS.items():
        result = bq.client.query(bq.render(sql)).result()
        cols = [f.name for f in result.schema]
        header = [c.upper() for c in cols]   # Snowflake exports used capitals, so the Tableau workbook expects them
        rows = [
            [("'" + str(row[c])) if c in TEXT_COLUMNS and row[c] is not None else format_cell(row[c])
             for c in cols]
            for row in result
        ]
        write_tab(session, sheet_id, title, header, rows)
        counts[title] = len(rows)
        logger.info("[sheet] %s: %d rows", title, len(rows))
    return counts
