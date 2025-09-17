# sample_queries.py
import duckdb
from src import config

con = duckdb.connect(config.DB_FILE.as_posix(), read_only=True)

def view_exists(view_name: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.views WHERE table_name = ?",
        [view_name]
    ).fetchone()
    return bool(row and row[0] > 0)

def print_count(label: str, view: str):
    if not view_exists(view):
        print(f"{label}: view {view} not found (run the importer first).")
        return
    n = con.execute(f"SELECT COUNT(*) AS n FROM {view}").fetchone()[0]
    print(f"{label}: {n:,} rows")

def print_date_range(label: str, view: str, date_col: str = "date"):
    if not view_exists(view):
        print(f"{label} range: view {view} not found.")
        return
    # Guard: if the view exists but has no rows, skip
    empty = con.execute(f"SELECT COUNT(*)=0 FROM {view}").fetchone()[0]
    if empty:
        print(f"{label} range: (no rows)")
        return
    # Use arg_min/arg_max to fetch an associated symbol for min/max date
    q = f"""
        SELECT
          MIN({date_col})   AS min_date,
          ARG_MIN(symbol, {date_col}) AS min_symbol,
          MAX({date_col})   AS max_date,
          ARG_MAX(symbol, {date_col}) AS max_symbol,
          COUNT(*)          AS total_rows,
          COUNT(DISTINCT symbol) AS distinct_symbols
        FROM {view}
    """
    df = con.execute(q).fetchdf()
    row = df.iloc[0]
    print(
        f"{label} range: "
        f"min={row.min_date.date() if hasattr(row.min_date, 'date') else row.min_date} "
        f"(symbol={row.min_symbol}), "
        f"max={row.max_date.date() if hasattr(row.max_date, 'date') else row.max_date} "
        f"(symbol={row.max_symbol}); "
        f"rows={row.total_rows:,}, symbols={row.distinct_symbols:,}"
    )

print("\n-- Row counts --")
print_count("Constituents", "v_sp500_constituents")
print_count("Prices", "v_prices")
print_count("Key metrics", "v_key_metrics")
print_count("Ratios", "v_ratios")
print_count("Revenue segments", "v_revenue_segments")

print("\n-- Latest ROIC top 10 (key metrics) --")
if view_exists("v_key_metrics"):
    print(con.execute("""
        SELECT symbol, date, returnOnInvestedCapital
        FROM v_key_metrics
        QUALIFY row_number() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
        ORDER BY returnOnInvestedCapital DESC NULLS LAST
        LIMIT 10
    """).fetchdf())
else:
    print("v_key_metrics not found (run fundamentals.py).")

print("\n-- Segment mix (most recent per symbol) --")
if view_exists("v_revenue_segments"):
    print(con.execute("""
        WITH latest AS (
          SELECT symbol, max(date) AS max_date FROM v_revenue_segments GROUP BY 1
        )
        SELECT r.symbol, r.segment, r.revenue,
               r.revenue / SUM(r.revenue) OVER (PARTITION BY r.symbol) AS segment_share
        FROM v_revenue_segments r
        JOIN latest l ON r.symbol = l.symbol AND r.date = l.max_date
        ORDER BY r.symbol, segment_share DESC
    """).fetchdf())
else:
    print("v_revenue_segments not found (run fundamentals.py).")

print("\n-- Data sparsity / date coverage snapshot --")
print_date_range("Prices", "v_prices", "date")
print_date_range("Key metrics", "v_key_metrics", "date")
print_date_range("Ratios", "v_ratios", "date")
print_date_range("Revenue segments", "v_revenue_segments", "date")
