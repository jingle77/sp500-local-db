from __future__ import annotations
import os, uuid
import pandas as pd

def upsert_parquet(df_new: pd.DataFrame, path: str, key_cols: list[str]) -> int:
    """
    Upsert df_new into parquet at path using key_cols as unique key.
    Returns number of newly added rows after dedup (updated rows count as 0 added).
    """
    if df_new is None or df_new.empty:
        return 0

    if os.path.exists(path):
        df_old = pd.read_parquet(path)
        before = len(df_old)
        df = pd.concat([df_old, df_new], ignore_index=True)
        df = df.drop_duplicates(subset=key_cols, keep="last")
        added = len(df) - before
    else:
        df = df_new
        added = len(df)

    tmp = f"{path}.{uuid.uuid4().hex}.tmp"
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)  # atomic replace
    return max(0, added)
