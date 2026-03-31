from __future__ import annotations

import pandas as pd


def build_summary(
    platform: pd.DataFrame,
    bank: pd.DataFrame,
    payouts: pd.DataFrame,
    reconcile_result: dict,
    anomalies: list[dict],
) -> dict:
    matches: pd.DataFrame = reconcile_result["matches"]
    um_p = reconcile_result["unmatched_platform"]
    um_b = reconcile_result["unmatched_bank"]
    partial = reconcile_result["partial_matches"]

    plat_total = float(pd.to_numeric(platform["amount"], errors="coerce").fillna(0).sum())
    bank_total = float(pd.to_numeric(bank["amount"], errors="coerce").fillna(0).sum())
    payout_total = float(pd.to_numeric(payouts["amount"], errors="coerce").fillna(0).sum())

    matched_count = len(matches)
    exact = int((matches["match_type"] == "exact_id").sum()) if len(matches) else 0
    fallback = int((matches["match_type"] == "fallback_amount_window").sum()) if len(matches) else 0

    return {
        "counts": {
            "platform_rows": len(platform),
            "bank_rows": len(bank),
            "payout_rows": len(payouts),
            "matched_pairs": matched_count,
            "matched_exact_id": exact,
            "matched_fallback": fallback,
            "unmatched_platform": len(um_p),
            "unmatched_bank": len(um_b),
            "partial_amount_mismatch": len(partial),
        },
        "totals": {
            "platform_amount": plat_total,
            "bank_amount": bank_total,
            "platform_minus_bank": plat_total - bank_total,
            "payout_amount": payout_total,
            "payout_vs_bank_settlement": payout_total - bank_total,
        },
        "anomaly_counts_by_type": _count_anomaly_types(anomalies),
    }


def _count_anomaly_types(anomalies: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for a in anomalies:
        t = a.get("type") or "unknown"
        out[t] = out.get(t, 0) + 1
    return out


def monthly_report(platform: pd.DataFrame, matches: pd.DataFrame) -> list[dict]:
    if not len(platform):
        return []
    plat = platform.copy()
    plat["_plat_ord"] = range(len(plat))
    plat["_ym"] = plat["created_at"].dt.to_period("M")
    out = []
    for period, g in plat.groupby("_ym", dropna=False):
        ym = str(period) if pd.notna(period) else "unknown"
        sub_total = float(pd.to_numeric(g["amount"], errors="coerce").fillna(0).sum())
        ord_set = set(g["_plat_ord"].tolist())
        matched_in_month = 0
        if len(matches) and "platform_idx" in matches.columns:
            matched_in_month = int(matches[matches["platform_idx"].isin(ord_set)].shape[0])
        out.append(
            {
                "month": ym,
                "platform_row_count": len(g),
                "platform_amount": sub_total,
                "matched_in_month": matched_in_month,
            }
        )
    return out
