# tests/test_fundamentals.py
import duckdb
import pytest
from src import config

def _view_exists(con, view_name: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.views WHERE table_name = ?",
        [view_name]
    ).fetchone()
    return bool(row and row[0] > 0)

def _columns(con, view_name: str) -> set[str]:
    # DESCRIBE returns columns and types; we only need names.
    return {r[0] for r in con.execute(f"DESCRIBE {view_name}").fetchall()}

def _rowcount(con, view_name: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]

@pytest.fixture(scope="module")
def con():
    c = duckdb.connect(config.DB_FILE.as_posix(), read_only=True)
    yield c
    c.close()

@pytest.mark.parametrize("view_name, expected_cols", [
    # Prices (from prices.py)
    ("v_prices", {
        "symbol", "date", "adjOpen", "adjHigh", "adjLow", "adjClose", "volume"
    }),
    # Fundamentals
    ("v_balance_sheet", {
        "symbol", "date", "period", "fiscalYear",
        "reportedCurrency",
        # a few canonical balance sheet fields
        "totalAssets", "totalLiabilities", "totalStockholdersEquity"
    }),
    ("v_income_statement", {
        "symbol", "date", "period", "fiscalYear",
        "reportedCurrency",
        # common IS fields
        "revenue", "grossProfit", "operatingIncome", "netIncome"
    }),
    ("v_key_metrics", {
        "symbol", "date", "period", "fiscalYear",
        # common metrics
        "marketCap", "enterpriseValue", "returnOnInvestedCapital", "currentRatio"
    }),
    ("v_ratios", {
        "symbol", "date", "period", "fiscalYear",
        # common ratios
        "grossProfitMargin", "netProfitMargin",
        "currentRatio", "quickRatio", "priceToEarningsRatio"
    }),
    ("v_revenue_segments", {
        "symbol", "date", "period", "fiscalYear",
        "reportedCurrency",
        "segment", "revenue"
    }),
])
def test_view_schemas_superset(con, view_name, expected_cols):
    # If the user hasn't run that importer yet, skip cleanly.
    if not _view_exists(con, view_name):
        pytest.skip(f"{view_name} not created yet (run the corresponding importer first)")

    cols = _columns(con, view_name)
    missing = expected_cols - cols
    assert not missing, f"{view_name} is missing expected columns: {sorted(missing)}"

    # If there are zero rows (e.g., user ran only symbols), don’t fail—just skip.
    n = _rowcount(con, view_name)
    if n == 0:
        pytest.skip(f"{view_name} exists but has no rows yet")

@pytest.mark.parametrize("view_name, key_cols", [
    # Natural keys used in our upserts
    ("v_prices", ["symbol", "date"]),
    ("v_balance_sheet", ["symbol", "date", "period"]),
    ("v_income_statement", ["symbol", "date", "period"]),
    ("v_key_metrics", ["symbol", "date", "period"]),
    ("v_ratios", ["symbol", "date", "period"]),
    ("v_revenue_segments", ["symbol", "date", "segment"]),
])
def test_keys_present(con, view_name, key_cols):
    if not _view_exists(con, view_name):
        pytest.skip(f"{view_name} not created yet")
    cols = _columns(con, view_name)
    missing = set(key_cols) - cols
    assert not missing, f"{view_name} is missing key columns: {sorted(missing)}"
