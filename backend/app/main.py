from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.serialize import dataframe_to_records
from app.service import PipelineState, load_state_from_disk

app = FastAPI(title="Payments Reconciliation API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_state: PipelineState | None = None


def get_state() -> PipelineState:
    global _state
    if _state is None:
        _state = load_state_from_disk()
    return _state


@app.on_event("startup")
def startup() -> None:
    get_state()


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/platform-transactions")
def api_platform() -> dict:
    st = get_state()
    return {
        "records": dataframe_to_records(st.platform),
        "row_count": len(st.platform),
    }


@app.get("/api/bank-settlements")
def api_bank() -> dict:
    st = get_state()
    return {
        "records": dataframe_to_records(st.bank),
        "row_count": len(st.bank),
    }


@app.get("/api/payouts")
def api_payouts() -> dict:
    st = get_state()
    return {
        "records": dataframe_to_records(st.payouts),
        "row_count": len(st.payouts),
    }


@app.get("/api/reconciliation/summary")
def api_reconciliation_summary() -> dict:
    st = get_state()
    return st.summary_payload()


@app.get("/api/reconciliation/matches")
def api_matches() -> dict:
    st = get_state()
    if st.reconcile_result is None:
        st.run()
    assert st.reconcile_result is not None
    m = st.reconcile_result["matches"]
    return {"records": dataframe_to_records(m), "row_count": len(m)}


@app.post("/api/ingest")
async def api_ingest(
    platform_transactions: UploadFile | None = File(None),
    bank_settlements: UploadFile | None = File(None),
    payouts: UploadFile | None = File(None),
) -> dict:
    global _state
    import pandas as pd

    from app.pipeline.ingestion import load_csv_bytes

    base = get_state()
    pr = base.platform_raw.copy()
    br = base.bank_raw.copy()
    por = base.payouts_raw.copy()

    if platform_transactions is not None:
        raw = await platform_transactions.read()
        if not raw:
            raise HTTPException(400, "Empty platform_transactions file")
        pr = load_csv_bytes(raw)
    if bank_settlements is not None:
        raw = await bank_settlements.read()
        if not raw:
            raise HTTPException(400, "Empty bank_settlements file")
        br = load_csv_bytes(raw)
    if payouts is not None:
        raw = await payouts.read()
        if not raw:
            raise HTTPException(400, "Empty payouts file")
        por = load_csv_bytes(raw)

    st = PipelineState(platform_raw=pr, bank_raw=br, payouts_raw=por)
    st.run()
    _state = st
    return {"ok": True, "summary": st.summary_payload()["summary"]}


@app.post("/api/reload-default")
def api_reload_default(data_dir: str | None = None) -> dict:
    global _state
    base = Path(data_dir) if data_dir else settings.data_dir
    _state = load_state_from_disk(base)
    return {"ok": True, "data_dir": str(base)}
