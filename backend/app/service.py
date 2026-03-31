from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from app.config import settings
from app.pipeline.anomalies import detect_anomalies
from app.pipeline.ingestion import load_csv_path
from app.pipeline.normalize import normalize_all
from app.pipeline.reconcile import run_reconciliation
from app.pipeline.reporting import build_summary, monthly_report
from app.serialize import dataframe_to_records


@dataclass
class PipelineState:
    platform_raw: pd.DataFrame = field(default_factory=pd.DataFrame)
    bank_raw: pd.DataFrame = field(default_factory=pd.DataFrame)
    payouts_raw: pd.DataFrame = field(default_factory=pd.DataFrame)
    platform: pd.DataFrame = field(default_factory=pd.DataFrame)
    bank: pd.DataFrame = field(default_factory=pd.DataFrame)
    payouts: pd.DataFrame = field(default_factory=pd.DataFrame)
    reconcile_result: dict | None = None
    anomalies: list[dict] = field(default_factory=list)

    def run(self) -> None:
        self.platform, self.bank, self.payouts = normalize_all(
            self.platform_raw, self.bank_raw, self.payouts_raw
        )
        self.reconcile_result = run_reconciliation(self.platform, self.bank)
        self.anomalies = detect_anomalies(self.platform, self.bank, self.reconcile_result)

    def summary_payload(self) -> dict:
        if self.reconcile_result is None:
            self.run()
        assert self.reconcile_result is not None
        summary = build_summary(
            self.platform,
            self.bank,
            self.payouts,
            self.reconcile_result,
            self.anomalies,
        )
        matches = self.reconcile_result["matches"]
        return {
            "summary": summary,
            "anomalies": self.anomalies,
            "gaps": {
                "unmatched_platform": dataframe_to_records(self.reconcile_result["unmatched_platform"]),
                "unmatched_bank": dataframe_to_records(self.reconcile_result["unmatched_bank"]),
                "partial_matches": dataframe_to_records(self.reconcile_result["partial_matches"]),
            },
            "matches_sample": dataframe_to_records(matches.head(500)),
            "monthly": monthly_report(self.platform, matches),
        }


def default_csv_paths(base: Path | None = None) -> tuple[Path, Path, Path]:
    root = base or settings.data_dir
    return (
        root / "platform_transactions.csv",
        root / "bank_settlements.csv",
        root / "payouts.csv",
    )


def load_state_from_disk(base: Path | None = None) -> PipelineState:
    p_plat, p_bank, p_po = default_csv_paths(base)
    st = PipelineState(
        platform_raw=load_csv_path(p_plat),
        bank_raw=load_csv_path(p_bank),
        payouts_raw=load_csv_path(p_po),
    )
    st.run()
    return st
