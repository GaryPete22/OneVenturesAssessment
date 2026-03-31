"""Tests for anomaly detectors (one scenario per anomaly type)."""

import pandas as pd

from app.pipeline.anomalies import detect_anomalies
from app.pipeline.normalize import normalize_all
from app.pipeline.reconcile import run_reconciliation


def _norm(platform_csv: str, bank_csv: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    from io import StringIO

    p = pd.read_csv(StringIO(platform_csv))
    b = pd.read_csv(StringIO(bank_csv))
    plat, bank, _ = normalize_all(p, b, pd.DataFrame())
    rec = run_reconciliation(plat, bank)
    return plat, bank, rec


def test_duplicate_settlement_flagged():
    platform = """transaction_id,user_id,amount,created_at,status
T1,100,100.0,2026-03-01 10:00:00,success
"""
    bank = """transaction_id,amount,created_at,bank_ref_id,settled_at,batch_id
T1,100.0,2026-03-01 10:00:00,BR1,2026-03-02 10:00:00,B1
T1,100.0,2026-03-01 10:00:00,BR2,2026-03-03 10:00:00,B2
"""
    plat, bnk, rec = _norm(platform, bank)
    anomalies = detect_anomalies(plat, bnk, rec)
    types = {a["type"] for a in anomalies}
    assert "duplicate_settlement" in types


def test_settled_next_month():
    platform = """transaction_id,user_id,amount,created_at,status
T1,100,100.0,2026-03-28 10:00:00,success
"""
    bank = """transaction_id,amount,created_at,bank_ref_id,settled_at,batch_id
T1,100.0,2026-03-28 10:00:00,BR1,2026-04-02 10:00:00,B1
"""
    plat, bnk, rec = _norm(platform, bank)
    anomalies = detect_anomalies(plat, bnk, rec)
    assert any(a["type"] == "settled_next_month" for a in anomalies)


def test_aggregate_rounding_difference():
    platform = """transaction_id,user_id,amount,created_at,status
A1,0,100.004,2026-03-01 10:00:00,success
"""
    bank = """transaction_id,amount,created_at,bank_ref_id,settled_at,batch_id
A1,100.0,2026-03-01 10:00:00,BR1,2026-03-02 10:00:00,B1
"""
    plat, bnk, rec = _norm(platform, bank)
    anomalies = detect_anomalies(plat, bnk, rec)
    assert any(a["type"] == "aggregate_rounding_difference" for a in anomalies)


def test_refund_without_original_transaction():
    platform = """transaction_id,user_id,amount,created_at,status
RONLY,0,-50.0,2026-03-15 10:00:00,refunded
"""
    bank = """transaction_id,amount,created_at,bank_ref_id,settled_at,batch_id
"""
    plat, bnk, rec = _norm(platform, bank)
    anomalies = detect_anomalies(plat, bnk, rec)
    assert any(
        a["type"] == "refund_without_original_transaction" for a in anomalies
    )
