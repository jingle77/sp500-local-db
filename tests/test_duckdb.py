import duckdb
from src import config

def test_basic_views_exist():
    con = duckdb.connect(config.DB_FILE.as_posix())
    try:
        views = [r[0] for r in con.execute("SELECT table_name FROM information_schema.views").fetchall()]
        required = [
            "v_sp500_constituents","v_prices",
            "v_balance_sheet","v_income_statement","v_key_metrics","v_ratios","v_revenue_segments"
        ]
        for v in required:
            assert v in views
    finally:
        con.close()
