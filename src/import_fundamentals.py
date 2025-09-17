from __future__ import annotations
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable
import pandas as pd
import duckdb
from tqdm.auto import tqdm

from . import config
from .fundamentals_client import FundamentalsClient
from .merge_parquet import upsert_parquet

log = logging.getLogger(__name__)

def _load_symbols() -> list[str]:
    con = duckdb.connect(config.DB_FILE.as_posix())
    try:
        return [r[0] for r in con.execute("SELECT symbol FROM v_sp500_constituents ORDER BY symbol").fetchall()]
    finally:
        con.close()

def _to_df(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    for col in ("date","filingDate","acceptedDate"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    if "fiscalYear" in df.columns:
        df["fiscalYear"] = pd.to_numeric(df["fiscalYear"], errors="coerce").astype("Int64")
    # gentle numeric casts where obvious (leave others as-is to avoid surprises)
    return df

def _normalize_segments(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    rows = []
    for row in records:
        base = {
            "symbol": row.get("symbol"),
            "reportedCurrency": row.get("reportedCurrency"),
            "period": row.get("period"),
            "fiscalYear": row.get("fiscalYear"),
            "date": row.get("date"),
        }
        data = row.get("data") or {}
        for seg, rev in data.items():
            r = base.copy()
            r["segment"] = seg
            r["revenue"] = rev
            rows.append(r)
    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["fiscalYear"] = pd.to_numeric(df["fiscalYear"], errors="coerce").astype("Int64")
        df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    return df

def _fetch_upsert(dataset_name: str,
                  getter: Callable[[FundamentalsClient, str], list[dict]],
                  out_dir,
                  key_cols: list[str],
                  normalize: Callable[[list[dict]], pd.DataFrame] | None,
                  max_workers: int):
    client = FundamentalsClient()
    symbols = _load_symbols()

    totals = {"dataset": dataset_name, "symbols_done": 0, "rows_added": 0, "files_touched": 0}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(getter, client, s): s for s in symbols}
        for fut in tqdm(as_completed(futs), total=len(futs), desc=dataset_name):
            sym = futs[fut]
            try:
                data = fut.result()
            except Exception as e:
                log.exception("[%s] error %s: %s", dataset_name, sym, e)
                continue
            df = normalize(data) if normalize else _to_df(data)
            if df.empty:
                continue
            if "symbol" not in df.columns:
                df["symbol"] = sym
            out_path = (out_dir / f"{sym}.parquet").as_posix()
            added = upsert_parquet(df, out_path, key_cols=key_cols)
            totals["rows_added"] += added
            totals["files_touched"] += 1
            totals["symbols_done"] += 1
    return totals

def import_balance_sheet(max_workers: int = config.MAX_WORKERS):
    return _fetch_upsert("balance_sheet",
                         FundamentalsClient.balance_sheet,
                         config.PARQUET_BALANCE_SHEET_DIR,
                         ["symbol","date","period"],
                         None,
                         max_workers)

def import_income_statement(max_workers: int = config.MAX_WORKERS):
    return _fetch_upsert("income_statement",
                         FundamentalsClient.income_statement,
                         config.PARQUET_INCOME_STATEMENT_DIR,
                         ["symbol","date","period"],
                         None,
                         max_workers)

def import_key_metrics(max_workers: int = config.MAX_WORKERS):
    return _fetch_upsert("key_metrics",
                         FundamentalsClient.key_metrics,
                         config.PARQUET_KEY_METRICS_DIR,
                         ["symbol","date","period"],
                         None,
                         max_workers)

def import_ratios(max_workers: int = config.MAX_WORKERS):
    return _fetch_upsert("ratios",
                         FundamentalsClient.ratios,
                         config.PARQUET_RATIOS_DIR,
                         ["symbol","date","period"],
                         None,
                         max_workers)

def import_revenue_segments(max_workers: int = config.MAX_WORKERS):
    return _fetch_upsert("revenue_segments",
                         FundamentalsClient.revenue_segments,
                         config.PARQUET_REVENUE_SEGMENTS_DIR,
                         ["symbol","date","segment"],
                         _normalize_segments,
                         max_workers)
