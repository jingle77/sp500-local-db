# src/storage.py
from __future__ import annotations
import duckdb
from pathlib import Path
from . import config

def _sql_quote_path(p: Path) -> str:
    # Use forward slashes and escape single quotes for SQL string
    return p.as_posix().replace("'", "''")

def _create_view_for_parquet(con: duckdb.DuckDBPyConnection, view_name: str, pattern: Path):
    pattern_sql = _sql_quote_path(pattern)
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{pattern_sql}')")

def _maybe_create_view_for_dir(con: duckdb.DuckDBPyConnection, view_name: str, dir_path: Path):
    # Only create the view if there are files to read; otherwise drop it if it exists.
    has_files = any(dir_path.glob("*.parquet"))
    if has_files:
        _create_view_for_parquet(con, view_name, dir_path / "*.parquet")
    else:
        con.execute(f"DROP VIEW IF EXISTS {view_name}")

def ensure_db():
    con = duckdb.connect(config.DB_FILE.as_posix())

    # Constituents view (single file always exists after main.py run)
    _create_view_for_parquet(con, "v_sp500_constituents", config.PARQUET_CONSTITUENTS)

    # Prices & fundamentals: create only if there are files yet
    _maybe_create_view_for_dir(con, "v_prices", config.PARQUET_PRICES_DIR)
    _maybe_create_view_for_dir(con, "v_balance_sheet", config.PARQUET_BALANCE_SHEET_DIR)
    _maybe_create_view_for_dir(con, "v_income_statement", config.PARQUET_INCOME_STATEMENT_DIR)
    _maybe_create_view_for_dir(con, "v_key_metrics", config.PARQUET_KEY_METRICS_DIR)
    _maybe_create_view_for_dir(con, "v_ratios", config.PARQUET_RATIOS_DIR)
    _maybe_create_view_for_dir(con, "v_revenue_segments", config.PARQUET_REVENUE_SEGMENTS_DIR)

    con.close()
