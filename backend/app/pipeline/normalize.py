from __future__ import annotations

import pandas as pd


def _parse_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def normalize_platform(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().lower() for c in out.columns]
    amt = _num(out.get("amount", pd.Series(dtype=float)))
    uid = _num(out.get("user_id", pd.Series(dtype=float)))
    # Some exports put monetary amount in user_id when amount is empty
    inferred = amt.isna() & uid.notna()
    out["amount"] = amt.fillna(uid)
    out["amount_inferred_from_user_id"] = inferred.fillna(False)
    out["created_at"] = _parse_dt(out.get("created_at"))
    out["status"] = out.get("status", pd.Series(dtype=str)).astype(str).str.lower().str.strip()
    out["transaction_id"] = out.get("transaction_id", pd.Series(dtype=str)).astype(str).str.strip()
    out["is_refund_like"] = out["status"].str.contains("refund", na=False) | (out["amount"] < 0)
    return out


def normalize_bank(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().lower() for c in out.columns]
    out["amount"] = _num(out.get("amount", pd.Series(dtype=float)))
    out["created_at"] = _parse_dt(out.get("created_at"))
    out["settled_at"] = _parse_dt(out.get("settled_at"))
    out["transaction_id"] = out.get("transaction_id", pd.Series(dtype=str)).astype(str).str.strip()
    out["bank_ref_id"] = out.get("bank_ref_id", pd.Series(dtype=str)).astype(str)
    out["batch_id"] = out.get("batch_id", pd.Series(dtype=str)).astype(str)
    return out


def normalize_payouts(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().lower() for c in out.columns]
    out["amount"] = _num(out.get("amount", pd.Series(dtype=float)))
    out["payout_date"] = _parse_dt(out.get("payout_date"))
    out["payout_id"] = out.get("payout_id", pd.Series(dtype=str)).astype(str)
    out["merchant_id"] = out.get("merchant_id", pd.Series(dtype=str)).astype(str)
    out["status"] = out.get("status", pd.Series(dtype=str)).astype(str).str.lower().str.strip()
    return out


def normalize_all(
    platform: pd.DataFrame,
    bank: pd.DataFrame,
    payouts: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return (
        normalize_platform(platform),
        normalize_bank(bank),
        normalize_payouts(payouts),
    )
