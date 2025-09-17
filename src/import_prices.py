from __future__ import annotations
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import duckdb, pandas as pd
from tqdm.auto import tqdm

from . import config
from .merge_parquet import upsert_parquet
from .prices_client import PricesClient

log = logging.getLogger(__name__)

def _load_symbols() -> list[str]:
    con = duckdb.connect(config.DB_FILE.as_posix())
    try:
        rows = con.execute("SELECT symbol FROM v_sp500_constituents ORDER BY symbol").fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()

def _records_to_df(symbol: str, records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    # enforce known columns; create if missing
    keep = ["date","adjOpen","adjHigh","adjLow","adjClose","volume"]
    for c in keep:
        if c not in df.columns:
            df[c] = pd.NA
    df["symbol"] = symbol
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    # cast numerics gently
    for c in ["adjOpen","adjHigh","adjLow","adjClose","volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["symbol"] + keep].dropna(subset=["date"])

def import_prices(max_workers: int = config.MAX_WORKERS) -> dict:
    symbols = _load_symbols()
    client = PricesClient()

    totals = {"symbols_done":0, "rows_added":0, "files_touched":0}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(client.historical_divadj, s): s for s in symbols}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="Prices"):
            sym = futs[fut]
            try:
                data = fut.result()
            except Exception as e:
                log.exception("Error %s: %s", sym, e)
                continue
            df = _records_to_df(sym, data)
            if df.empty:
                continue
            out_path = (config.PARQUET_PRICES_DIR / f"{sym}.parquet").as_posix()
            added = upsert_parquet(df, out_path, key_cols=["symbol","date"])
            totals["rows_added"] += added
            totals["files_touched"] += 1
            totals["symbols_done"] += 1
    return totals
