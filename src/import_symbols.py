from __future__ import annotations
import logging
import pandas as pd
import requests
from . import config

log = logging.getLogger(__name__)

def fetch_sp500_constituents() -> pd.DataFrame:
    r = requests.get(
        config.FMP_CONSTITUENTS_URL,
        params={"apikey": config.API_KEY},
        timeout=30
    )
    r.raise_for_status()
    data = r.json() or []
    df = pd.DataFrame(data)

    # normalize expected columns
    expected = ["symbol","name","sector","subSector","headQuarter","dateFirstAdded","cik","founded"]
    for c in expected:
        if c not in df.columns:
            df[c] = None

    df.to_parquet(config.PARQUET_CONSTITUENTS, index=False)  # snapshot overwrite
    return df
