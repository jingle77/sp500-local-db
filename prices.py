import logging
from src import storage
from src.import_prices import import_prices

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("prices")

if __name__ == "__main__":
    storage.ensure_db()  # ensure v_sp500_constituents exists
    res = import_prices()
    log.info("Prices import summary: %s", res)
    storage.ensure_db()
