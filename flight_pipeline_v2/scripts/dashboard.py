"""
Dashboard: the last DAG task, running only after snowflake_load succeeds.
Queries the two ANALYTICS views and renders a static, self-contained HTML
report with inline SVG bar charts - no charting library, no internet
connection needed to view it, just a browser pointed at the output file.

Regenerated every 30 minutes alongside the rest of the pipeline, so
data/dashboard/latest.html always reflects what's actually in Snowflake
right now, not a stale snapshot from whenever someone last looked.
"""
import html
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import snowflake.connector

logger = logging.getLogger(__name__)

TOP_N = 10

COUNTRY_SUMMARY_SQL = """
    SELECT flight_date, origin_country, total_flights, avg_velocity,
           peak_velocity, avg_altitude, total_on_ground, rank_by_volume
    FROM FLIGHTS.ANALYTICS.COUNTRY_DAILY_SUMMARY
    WHERE flight_date = CURRENT_DATE()
    ORDER BY total_flights DESC
    LIMIT %s
"""

ROLLING_24H_SQL = """
    SELECT origin_country, flights_last_24h, avg_velocity_last_24h, last_loaded_at
    FROM FLIGHTS.ANALYTICS.ROLLING_24H_ACTIVITY
    ORDER BY flights_last_24h DESC
    LIMIT %s
"""


def _query_df(conn, sql: str, params: tuple) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        columns = [c[0].lower() for c in cursor.description]
        rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)


def fetch_dashboard_data(connection_params: dict) -> dict:
    """Pulls the two ANALYTICS views back out of Snowflake, right after the
    pipeline just wrote to them - this is what proves the load actually
    worked, not just that the task returned success."""
    conn = snowflake.connector.connect(**connection_params)
    try:
        country_summary = _query_df(conn, COUNTRY_SUMMARY_SQL, (TOP_N,))
        rolling_24h = _query_df(conn, ROLLING_24H_SQL, (TOP_N,))
    finally:
        conn.close()
    return {"country_summary": country_summary, "rolling_24h": rolling_24h}


def _bar_chart_svg(labels, values, color, unit="", width=620, row_height=32) -> str:
    """Pure-Python horizontal bar chart as inline SVG - no matplotlib/plotly
    dependency, just proportional geometry, so this has zero extra
    requirements beyond what's already installed."""
    if not values:
        return '<p class="empty">No data yet.</p>'
    max_value = max(values) or 1
    label_col = 150
    bar_col = width - label_col - 70
    height = row_height * len(values) + 10
    bars = []
    for i, (label, value) in enumerate(zip(labels, values)):
        y = i * row_height + 6
        bar_w = max((value / max_value) * bar_col, 2)
        safe_label = html.escape(str(label))[:22]
        bars.append(
            f'<text x="{label_col - 10}" y="{y + row_height / 2 + 4}" text-anchor="end" '
            f'class="bar-label">{safe_label}</text>'
            f'<rect x="{label_col}" y="{y}" width="{bar_w:.1f}" height="{row_height - 10}" '
            f'rx="4" fill="{color}"></rect>'
            f'<text x="{label_col + bar_w + 8}" y="{y + row_height / 2 + 4}" class="bar-value">'
            f'{value:,.0f}{unit}</text>'
        )
    return f'<svg viewBox="0 0 {width} {height}" width="100%">{"".join(bars)}</svg>'


def _table_html(df: pd.DataFrame, columns: list, headers: list) -> str:
    if df.empty:
        return '<p class="empty">No data yet.</p>'
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body_rows = []
    for _, row in df.iterrows():
        cells = "".join(f"<td>{html.escape(str(row[c]))}</td>" for c in columns)
        body_rows.append(f"<tr>{cells}</tr>")
    return f'<table><thead><tr>{head}</tr></thead><tbody>{"".join(body_rows)}</tbody></table>'


def render_dashboard_html(country_summary: pd.DataFrame, rolling_24h: pd.DataFrame, generated_at: datetime) -> str:
    """Pure function: two DataFrames in, a complete HTML document out. No
    Snowflake/Airflow dependency, so this is unit-testable with fixture
    DataFrames alone."""
    total_flights_today = int(country_summary["total_flights"].sum()) if not country_summary.empty else 0
    active_countries_today = int(country_summary["origin_country"].nunique()) if not country_summary.empty else 0
    flights_last_24h = int(rolling_24h["flights_last_24h"].sum()) if not rolling_24h.empty else 0
    last_loaded = rolling_24h["last_loaded_at"].max() if not rolling_24h.empty else None

    volume_chart = _bar_chart_svg(
        country_summary["origin_country"].tolist() if not country_summary.empty else [],
        country_summary["total_flights"].tolist() if not country_summary.empty else [],
        color="#f2a93b",
    )
    speed_chart = _bar_chart_svg(
        country_summary["origin_country"].tolist() if not country_summary.empty else [],
        country_summary["avg_velocity"].tolist() if not country_summary.empty else [],
        color="#57bfe0",
        unit=" m/s",
    )
    rolling_table = _table_html(
        rolling_24h,
        columns=["origin_country", "flights_last_24h", "avg_velocity_last_24h", "last_loaded_at"],
        headers=["Country", "Flights (24h)", "Avg velocity (24h)", "Last loaded"],
    )

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Flight Pipeline Dashboard</title>
<style>
  :root {{ --bg:#12161c; --surface:#1a2029; --border:#2c3542; --text:#e8ecf1; --muted:#96a2b3; --accent:#f2a93b; }}
  * {{ box-sizing: border-box; }}
  body {{ margin:0; background:var(--bg); color:var(--text); font-family:-apple-system,"Segoe UI",sans-serif; padding:36px; }}
  h1 {{ font-size:1.5rem; margin:0 0 4px; }}
  .meta {{ color:var(--muted); font-size:0.85rem; margin-bottom:28px; }}
  .kpis {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:12px; margin-bottom:28px; }}
  .kpi {{ background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:16px 18px; }}
  .kpi .n {{ font-size:1.6rem; font-weight:700; color:var(--accent); font-variant-numeric:tabular-nums; }}
  .kpi .l {{ font-size:0.78rem; color:var(--muted); margin-top:2px; }}
  .panel {{ background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:20px 22px; margin-bottom:20px; }}
  .panel h2 {{ font-size:1rem; margin:0 0 14px; }}
  .grid2 {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(420px,1fr)); gap:20px; }}
  .bar-label {{ fill:var(--muted); font-size:11px; font-family:inherit; }}
  .bar-value {{ fill:var(--text); font-size:11px; font-family:inherit; }}
  table {{ width:100%; border-collapse:collapse; font-size:0.85rem; }}
  th,td {{ text-align:left; padding:8px 10px; border-bottom:1px solid var(--border); }}
  th {{ color:var(--muted); font-weight:600; font-size:0.72rem; text-transform:uppercase; letter-spacing:0.04em; }}
  .empty {{ color:var(--muted); font-size:0.85rem; }}
  footer {{ color:var(--muted); font-size:0.78rem; margin-top:20px; }}
</style>
</head>
<body>
  <h1>Flight Pipeline Dashboard</h1>
  <p class="meta">Generated {generated_at.strftime("%Y-%m-%d %H:%M:%S UTC")} · source: FLIGHTS.ANALYTICS · regenerated every pipeline run</p>

  <div class="kpis">
    <div class="kpi"><div class="n">{total_flights_today:,}</div><div class="l">Flights today</div></div>
    <div class="kpi"><div class="n">{active_countries_today}</div><div class="l">Active countries today</div></div>
    <div class="kpi"><div class="n">{flights_last_24h:,}</div><div class="l">Flights, last 24h</div></div>
    <div class="kpi"><div class="n">{html.escape(str(last_loaded)) if last_loaded is not None else "—"}</div><div class="l">Last loaded (UTC)</div></div>
  </div>

  <div class="grid2">
    <div class="panel">
      <h2>Top {TOP_N} countries by flight volume today</h2>
      {volume_chart}
    </div>
    <div class="panel">
      <h2>Top {TOP_N} countries by average velocity today</h2>
      {speed_chart}
    </div>
  </div>

  <div class="panel">
    <h2>Rolling 24-hour activity</h2>
    {rolling_table}
  </div>

  <footer>flights_ops_medallion_pipe · generate_dashboard task</footer>
</body>
</html>
"""


def run_dashboard_task(**context):
    """Airflow task wrapper."""
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("flight_snowflake")
    connection_params = dict(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson["account"],
        warehouse=conn.extra_dejson.get("warehouse"),
        database="FLIGHTS",
        schema="ANALYTICS",
        role=conn.extra_dejson.get("role"),
    )

    data = fetch_dashboard_data(connection_params)
    generated_at = datetime.now(timezone.utc)
    report_html = render_dashboard_html(data["country_summary"], data["rolling_24h"], generated_at)

    dashboard_path = Path("/opt/airflow/data/dashboard")
    dashboard_path.mkdir(parents=True, exist_ok=True)
    (dashboard_path / "latest.html").write_text(report_html)

    logger.info(f"Dashboard written to {dashboard_path / 'latest.html'}")
    context["ti"].xcom_push(key="dashboard_file", value=str(dashboard_path / "latest.html"))
