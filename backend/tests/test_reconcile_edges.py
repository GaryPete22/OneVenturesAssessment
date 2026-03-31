"""Reconciliation edge cases: exact match, fallback, partial amount, missing IDs."""

import pandas as pd

from app.pipeline.normalize import normalize_all
from app.pipeline.reconcile import ReconcileConfig, run_reconciliation


def _run(platform_csv: str, bank_csv: str, cfg: ReconcileConfig | None = None):
    from io import StringIO

    p = pd.read_csv(StringIO(platform_csv))
    b = pd.read_csv(StringIO(bank_csv))
    plat, bank, _ = normalize_all(p, b, pd.DataFrame())
    return run_reconciliation(plat, bank, cfg)


def test_exact_match_by_transaction_id():
    platform = """transaction_id,user_id,amount,created_at,status
X1,99,99.5,2026-03-01 12:00:00,success
"""
    bank = """transaction_id,amount,created_at,bank_ref_id,settled_at,batch_id
X1,99.5,2026-03-01 12:00:00,BR0,2026-03-02 12:00:00,B0
"""
    r = _run(platform, bank)
    assert len(r["matches"]) == 1
    assert r["matches"].iloc[0]["match_type"] == "exact_id"
    assert len(r["unmatched_platform"]) == 0
    assert len(r["unmatched_bank"]) == 0


def test_fallback_amount_and_window():
    platform = """transaction_id,user_id,amount,created_at,status
,200,200.0,2026-03-01 12:00:00,success
"""
    bank = """transaction_id,amount,created_at,bank_ref_id,settled_at,batch_id
,200.0,2026-03-01 12:00:00,BR0,2026-03-03 12:00:00,B0
"""
    r = _run(platform, bank)
    assert len(r["matches"]) == 1
    assert r["matches"].iloc[0]["match_type"] == "fallback_amount_window"


def test_partial_match_amount_mismatch_same_id():
    platform = """transaction_id,user_id,amount,created_at,status
P1,100,100.0,2026-03-01 12:00:00,success
"""
    bank = """transaction_id,amount,created_at,bank_ref_id,settled_at,batch_id
P1,200.0,2026-03-01 12:00:00,BR0,2026-03-02 12:00:00,B0
"""
    r = _run(platform, bank)
    assert len(r["matches"]) == 1
    assert len(r["partial_matches"]) == 1
    assert r["partial_matches"].iloc[0]["issue"] == "amount_mismatch"


def test_missing_bank_for_id_stays_unmatched():
    platform = """transaction_id,user_id,amount,created_at,status
ONLYP,50,50.0,2026-03-01 12:00:00,success
"""
    bank = """transaction_id,amount,created_at,bank_ref_id,settled_at,batch_id
OTHER,50.0,2026-03-01 12:00:00,BR0,2026-03-12 12:00:00,B0
"""
    r = _run(platform, bank)
    assert len(r["matches"]) == 0
    assert len(r["unmatched_platform"]) == 1
    assert len(r["unmatched_bank"]) == 1


def test_multiple_platform_same_id_consumes_first_bank_only():
    platform = """transaction_id,user_id,amount,created_at,status
DUP,10,10.0,2026-03-01 12:00:00,success
DUP,10,10.0,2026-03-01 13:00:00,success
"""
    bank = """transaction_id,amount,created_at,bank_ref_id,settled_at,batch_id
DUP,10.0,2026-03-01 12:00:00,BR0,2026-03-02 12:00:00,B0
"""
    r = _run(platform, bank)
    assert len(r["matches"]) == 1
    assert len(r["unmatched_platform"]) == 1
