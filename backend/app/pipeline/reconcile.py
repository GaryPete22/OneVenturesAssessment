from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from app.config import settings


@dataclass
class ReconcileConfig:
    amount_tolerance: float = settings.amount_tolerance
    window_min_days: int = settings.settlement_window_min_days
    window_max_days: int = settings.settlement_window_max_days


def _empty_id(s: str) -> bool:
    return s is None or (isinstance(s, float) and pd.isna(s)) or str(s).strip() == "" or str(s).lower() == "nan"


def run_reconciliation(
    platform: pd.DataFrame,
    bank: pd.DataFrame,
    cfg: ReconcileConfig | None = None,
) -> dict:
    cfg = cfg or ReconcileConfig()
    plat = platform.copy()
    bnk = bank.copy()
    plat["_plat_idx"] = range(len(plat))
    bnk["_bank_idx"] = range(len(bnk))

    plat["tid_key"] = plat["transaction_id"].apply(lambda x: str(x).strip() if not _empty_id(x) else "")
    bnk["tid_key"] = bnk["transaction_id"].apply(lambda x: str(x).strip() if not _empty_id(x) else "")

    # Exact match on non-empty transaction_id (first bank row wins per platform row order)
    exact_rows: list[dict] = []
    used_bank: set[int] = set()
    used_plat: set[int] = set()

    # Map bank indices by tid for greedy assignment
    bank_by_tid: dict[str, list[int]] = {}
    for i, r in bnk.iterrows():
        tid = r["tid_key"]
        if tid:
            bank_by_tid.setdefault(tid, []).append(int(r["_bank_idx"]))

    for i, r in plat.iterrows():
        tid = r["tid_key"]
        if not tid:
            continue
        candidates = [j for j in bank_by_tid.get(tid, []) if j not in used_bank]
        if not candidates:
            continue
        j = candidates[0]
        used_bank.add(j)
        used_plat.add(int(r["_plat_idx"]))
        br = bnk[bnk["_bank_idx"] == j].iloc[0]
        exact_rows.append(
            {
                "platform_idx": int(r["_plat_idx"]),
                "bank_idx": j,
                "match_type": "exact_id",
                "transaction_id": tid,
                "platform_amount": r["amount"],
                "bank_amount": br["amount"],
                "platform_created_at": r["created_at"],
                "settled_at": br["settled_at"],
            }
        )

    unmatched_plat_idx = set(plat["_plat_idx"].astype(int)) - used_plat
    unmatched_bank_idx = set(bnk["_bank_idx"].astype(int)) - used_bank

    # Fallback: amount tolerance + settlement window (greedy, one-to-one)
    plat_um = plat[plat["_plat_idx"].isin(unmatched_plat_idx)].copy()
    bnk_um = bnk[bnk["_bank_idx"].isin(unmatched_bank_idx)].copy()

    fallback_rows: list[dict] = []
    plat_um = plat_um.sort_values("created_at", na_position="last")
    bnk_um = bnk_um.sort_values("settled_at", na_position="last")

    def in_window(pc: pd.Timestamp, st: pd.Timestamp) -> bool:
        if pd.isna(pc) or pd.isna(st):
            return False
        delta = (st.normalize() - pc.normalize()).days
        return cfg.window_min_days <= delta <= cfg.window_max_days

    bank_pool = bnk_um.to_dict("records")

    for _, pr in plat_um.iterrows():
        pidx = int(pr["_plat_idx"])
        if pidx in used_plat:
            continue
        pa = pr["amount"]
        if pd.isna(pa):
            continue
        best_j = None
        best_row = None
        for br in bank_pool:
            j = int(br["_bank_idx"])
            if j in used_bank:
                continue
            ba = br["amount"]
            if pd.isna(ba):
                continue
            if abs(float(pa) - float(ba)) > cfg.amount_tolerance:
                continue
            if not in_window(pr["created_at"], br["settled_at"]):
                continue
            best_j = j
            best_row = br
            break
        if best_j is not None:
            used_bank.add(best_j)
            used_plat.add(pidx)
            fallback_rows.append(
                {
                    "platform_idx": pidx,
                    "bank_idx": best_j,
                    "match_type": "fallback_amount_window",
                    "transaction_id": pr["tid_key"] or best_row["tid_key"] or "",
                    "platform_amount": pr["amount"],
                    "bank_amount": best_row["amount"],
                    "platform_created_at": pr["created_at"],
                    "settled_at": best_row["settled_at"],
                }
            )

    partial: list[dict] = []
    for row in exact_rows:
        pa, ba = row["platform_amount"], row["bank_amount"]
        if pd.notna(pa) and pd.notna(ba) and abs(float(pa) - float(ba)) > cfg.amount_tolerance:
            partial.append({**row, "issue": "amount_mismatch"})

    # Back-fill amounts from counterparty when one side missing (after partial detection)
    for row in exact_rows:
        pa, ba = row["platform_amount"], row["bank_amount"]
        if pd.isna(pa) and pd.notna(ba):
            row["platform_amount"] = ba
        if pd.isna(ba) and pd.notna(pa):
            row["bank_amount"] = pa

    matches = pd.DataFrame(exact_rows + fallback_rows)
    if matches.empty:
        matches = pd.DataFrame(
            columns=[
                "platform_idx",
                "bank_idx",
                "match_type",
                "transaction_id",
                "platform_amount",
                "bank_amount",
                "platform_created_at",
                "settled_at",
            ]
        )

    unmatched_plat_idx = set(plat["_plat_idx"].astype(int)) - used_plat
    unmatched_bank_idx = set(bnk["_bank_idx"].astype(int)) - used_bank

    unmatched_platform = plat[plat["_plat_idx"].isin(unmatched_plat_idx)].drop(
        columns=["_plat_idx", "tid_key"], errors="ignore"
    )
    unmatched_bank = bnk[bnk["_bank_idx"].isin(unmatched_bank_idx)].drop(
        columns=["_bank_idx", "tid_key"], errors="ignore"
    )

    return {
        "matches": matches,
        "unmatched_platform": unmatched_platform,
        "unmatched_bank": unmatched_bank,
        "partial_matches": pd.DataFrame(partial),
        "used_platform_indices": used_plat,
        "used_bank_indices": used_bank,
    }
