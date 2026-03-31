from __future__ import annotations

import pandas as pd

from app.config import settings


def detect_anomalies(
    platform: pd.DataFrame,
    bank: pd.DataFrame,
    reconcile_result: dict,
) -> list[dict]:
    anomalies: list[dict] = []
    matches: pd.DataFrame = reconcile_result["matches"]

    # Duplicate settlements (same transaction_id appears on multiple bank rows)
    if "transaction_id" in bank.columns and len(bank):
        tid = bank["transaction_id"].astype(str).str.strip()
        nonempty = tid != ""
        vc = tid[nonempty].value_counts()
        dup_ids = vc[vc > 1].index.tolist()
        for txid in dup_ids:
            rows = bank[tid == txid]
            refs = []
            for _, r in rows.iterrows():
                refs.append(
                    {
                        "bank_ref_id": str(r.get("bank_ref_id", "")),
                        "settled_at": r["settled_at"].isoformat() if pd.notna(r.get("settled_at")) else None,
                        "batch_id": str(r.get("batch_id", "")),
                    }
                )
            anomalies.append(
                {
                    "type": "duplicate_settlement",
                    "severity": "high",
                    "transaction_id": txid,
                    "detail": f"Bank shows {len(rows)} settlement rows for this transaction_id",
                    "rows": refs,
                }
            )

    # Settled in next calendar month vs platform transaction month
    if len(matches):
        for _, m in matches.iterrows():
            pc, st = m.get("platform_created_at"), m.get("settled_at")
            if pd.isna(pc) or pd.isna(st):
                continue
            if pc.month != st.month or pc.year != st.year:
                # Flag only when settlement is strictly "next month" (or later month)
                if (st.year, st.month) > (pc.year, pc.month):
                    anomalies.append(
                        {
                            "type": "settled_next_month",
                            "severity": "medium",
                            "transaction_id": m.get("transaction_id"),
                            "detail": f"Platform month {pc.year}-{pc.month:02d}, settlement {st.year}-{st.month:02d}",
                            "platform_created_at": pc.isoformat(),
                            "settled_at": st.isoformat(),
                            "match_type": m.get("match_type"),
                        }
                    )

    # Aggregate rounding / penny drift
    plat_amt = pd.to_numeric(platform["amount"], errors="coerce").fillna(0).sum()
    bank_amt = pd.to_numeric(bank["amount"], errors="coerce").fillna(0).sum()
    diff = float(plat_amt - bank_amt)
    thr = settings.rounding_aggregate_threshold
    if 0 < abs(diff) < thr:
        anomalies.append(
            {
                "type": "aggregate_rounding_difference",
                "severity": "low",
                "transaction_id": None,
                "detail": f"Platform total {plat_amt:.4f} vs bank total {bank_amt:.4f} (diff {diff:.4f})",
                "diff": diff,
            }
        )

    # Refunds without original successful transaction (same transaction_id)
    if len(platform):
        success_ids = set(
            platform.loc[~platform["is_refund_like"], "transaction_id"].astype(str).str.strip()
        )
        refund_rows = platform[platform["is_refund_like"]]
        for _, r in refund_rows.iterrows():
            tid = str(r["transaction_id"]).strip()
            if not tid or tid.lower() == "nan":
                anomalies.append(
                    {
                        "type": "refund_missing_original",
                        "severity": "high",
                        "transaction_id": tid or None,
                        "detail": "Refund-like row with missing transaction_id",
                        "created_at": r["created_at"].isoformat() if pd.notna(r.get("created_at")) else None,
                    }
                )
                continue
            if tid not in success_ids:
                anomalies.append(
                    {
                        "type": "refund_without_original_transaction",
                        "severity": "high",
                        "transaction_id": tid,
                        "detail": "No non-refund platform row shares this transaction_id",
                        "created_at": r["created_at"].isoformat() if pd.notna(r.get("created_at")) else None,
                    }
                )

    return anomalies
