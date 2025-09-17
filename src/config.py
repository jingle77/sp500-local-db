from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PARQUET_DIR = DATA_DIR / "parquet"
DB_DIR = ROOT / "db"
DB_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

DB_FILE = DB_DIR / "market.duckdb"

# Datasets
PARQUET_CONSTITUENTS = PARQUET_DIR / "sp500_constituents.parquet"
PARQUET_PRICES_DIR = PARQUET_DIR / "prices"
PARQUET_BALANCE_SHEET_DIR = PARQUET_DIR / "balance_sheet"
PARQUET_INCOME_STATEMENT_DIR = PARQUET_DIR / "income_statement"
PARQUET_KEY_METRICS_DIR = PARQUET_DIR / "key_metrics"
PARQUET_RATIOS_DIR = PARQUET_DIR / "ratios"
PARQUET_REVENUE_SEGMENTS_DIR = PARQUET_DIR / "revenue_segments"

for p in [
    PARQUET_PRICES_DIR, PARQUET_BALANCE_SHEET_DIR, PARQUET_INCOME_STATEMENT_DIR,
    PARQUET_KEY_METRICS_DIR, PARQUET_RATIOS_DIR, PARQUET_REVENUE_SEGMENTS_DIR
]:
    p.mkdir(parents=True, exist_ok=True)

# Load API key and perf settings
load_dotenv(ROOT / ".env")
API_KEY = os.getenv("API_KEY", "").strip()
if not API_KEY:
    raise RuntimeError("Missing API_KEY in .env")

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "16"))
REQUESTS_PER_MINUTE = int(os.getenv("REQUESTS_PER_MINUTE", "700"))  # headroom < 750

# FMP endpoints
FMP_CONSTITUENTS_URL = "https://financialmodelingprep.com/stable/sp500-constituent"
FMP_PRICES_URL = "https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted"
FMP_BALANCE_SHEET_URL = "https://financialmodelingprep.com/stable/balance-sheet-statement"
FMP_INCOME_STATEMENT_URL = "https://financialmodelingprep.com/stable/income-statement"
FMP_KEY_METRICS_URL = "https://financialmodelingprep.com/stable/key-metrics"
FMP_RATIOS_URL = "https://financialmodelingprep.com/stable/ratios"
FMP_REVENUE_SEGMENTS_URL = "https://financialmodelingprep.com/stable/revenue-product-segmentation"
