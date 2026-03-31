from __future__ import annotations

import math

import pandas as pd


def dataframe_to_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = out[c].apply(lambda x: x.isoformat() if pd.notna(x) else None)
    records = out.to_dict(orient="records")
    for rec in records:
        for k, v in list(rec.items()):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                rec[k] = None
            elif v is pd.NA:
                rec[k] = None
    return records
