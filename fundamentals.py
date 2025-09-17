import logging
from src import storage
from src.import_fundamentals import (
    import_balance_sheet, import_income_statement,
    import_key_metrics, import_ratios, import_revenue_segments
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fundamentals")

if __name__ == "__main__":
    storage.ensure_db()
    for fn in [
        import_balance_sheet,
        import_income_statement,
        import_key_metrics,
        import_ratios,
        import_revenue_segments,
    ]:
        res = fn()
        log.info("%s", res)
        storage.ensure_db()
