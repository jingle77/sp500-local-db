import logging
from src import storage
from src.import_symbols import fetch_sp500_constituents

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("main")

if __name__ == "__main__":
    df = fetch_sp500_constituents()
    log.info("Constituents rows: %d", len(df))
    storage.ensure_db()
    log.info("DuckDB views refreshed.")
