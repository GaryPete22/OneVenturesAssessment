import { useCallback, useEffect, useMemo, useState } from "react";
import {
  fetchJson,
  type Anomaly,
  type SummaryPayload,
  type TableResponse,
} from "./api";
import "./App.css";

type Tab = "overview" | "transactions" | "settlements" | "payouts" | "gaps" | "monthly";

function severityColor(s?: string) {
  if (s === "high") return "var(--bad)";
  if (s === "medium") return "var(--warn)";
  return "var(--low)";
}

function AnomalyBadge({ a }: { a: Anomaly }) {
  return (
    <span
      className="anomaly-badge"
      style={{ borderColor: severityColor(a.severity) }}
      title={String(a.detail ?? "")}
    >
      <span className="dot" style={{ background: severityColor(a.severity) }} />
      {a.type?.replace(/_/g, " ")}
    </span>
  );
}

function DataTable({
  rows,
  maxHeight,
}: {
  rows: Record<string, unknown>[];
  maxHeight?: string;
}) {
  const keys = useMemo(() => {
    if (!rows.length) return [];
    return Object.keys(rows[0]);
  }, [rows]);
  if (!rows.length) {
    return <p className="muted">No rows</p>;
  }
  return (
    <div className="table-wrap" style={{ maxHeight }}>
      <table>
        <thead>
          <tr>
            {keys.map((k) => (
              <th key={k}>{k}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              {keys.map((k) => (
                <td key={k} className="mono">
                  {formatCell(r[k])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatCell(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

export default function App() {
  const [tab, setTab] = useState<Tab>("overview");
  const [summary, setSummary] = useState<SummaryPayload | null>(null);
  const [platform, setPlatform] = useState<TableResponse | null>(null);
  const [bank, setBank] = useState<TableResponse | null>(null);
  const [payouts, setPayouts] = useState<TableResponse | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setErr(null);
    setLoading(true);
    try {
      const [s, p, b, po] = await Promise.all([
        fetchJson<SummaryPayload>("/api/reconciliation/summary"),
        fetchJson<TableResponse>("/api/platform-transactions"),
        fetchJson<TableResponse>("/api/bank-settlements"),
        fetchJson<TableResponse>("/api/payouts"),
      ]);
      setSummary(s);
      setPlatform(p);
      setBank(b);
      setPayouts(po);
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Load failed");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const anomalyByTx = useMemo(() => {
    const m = new Map<string, Anomaly[]>();
    if (!summary?.anomalies) return m;
    for (const a of summary.anomalies) {
      const tid = a.transaction_id;
      if (tid && typeof tid === "string") {
        const list = m.get(tid) ?? [];
        list.push(a);
        m.set(tid, list);
      }
    }
    return m;
  }, [summary]);

  const txRowsFlagged = useMemo(() => {
    if (!platform?.records) return [];
    return platform.records.map((r) => {
      const row = r as Record<string, unknown>;
      const tid = String(row.transaction_id ?? "");
      const flags = anomalyByTx.get(tid) ?? [];
      return { ...row, _anomalies: flags };
    });
  }, [platform, anomalyByTx]);

  return (
    <div className="app">
      <header className="header">
        <div>
          <h1>Payments reconciliation</h1>
          <p className="subtitle">Platform · bank settlements · payouts</p>
        </div>
        <button type="button" className="btn" onClick={load} disabled={loading}>
          {loading ? "Loading…" : "Refresh"}
        </button>
      </header>

      {err && <div className="banner error">{err}</div>}

      <nav className="tabs">
        {(
          [
            ["overview", "Overview"],
            ["transactions", "Transactions"],
            ["settlements", "Settlement logs"],
            ["payouts", "Payouts"],
            ["gaps", "Reconciliation gaps"],
            ["monthly", "Monthly report"],
          ] as const
        ).map(([id, label]) => (
          <button
            key={id}
            type="button"
            className={tab === id ? "tab active" : "tab"}
            onClick={() => setTab(id)}
          >
            {label}
          </button>
        ))}
      </nav>

      <main className="main">
        {tab === "overview" && summary && (
          <section className="grid-overview">
            <div className="cards">
              <div className="card">
                <h3>Match quality</h3>
                <div className="stat">
                  <span className="label">Matched pairs</span>
                  <span className="value ok">
                    {summary.summary.counts.matched_pairs}
                  </span>
                </div>
                <div className="stat">
                  <span className="label">Exact ID</span>
                  <span className="value">
                    {summary.summary.counts.matched_exact_id}
                  </span>
                </div>
                <div className="stat">
                  <span className="label">Fallback (amount + window)</span>
                  <span className="value warn">
                    {summary.summary.counts.matched_fallback}
                  </span>
                </div>
                <div className="stat">
                  <span className="label">Unmatched platform / bank</span>
                  <span className="value">
                    {summary.summary.counts.unmatched_platform} /{" "}
                    {summary.summary.counts.unmatched_bank}
                  </span>
                </div>
              </div>
              <div className="card">
                <h3>Totals</h3>
                <div className="stat">
                  <span className="label">Platform amount</span>
                  <span className="value mono">
                    {summary.summary.totals.platform_amount.toLocaleString(
                      undefined,
                      { maximumFractionDigits: 2 }
                    )}
                  </span>
                </div>
                <div className="stat">
                  <span className="label">Bank amount</span>
                  <span className="value mono">
                    {summary.summary.totals.bank_amount.toLocaleString(
                      undefined,
                      { maximumFractionDigits: 2 }
                    )}
                  </span>
                </div>
                <div className="stat">
                  <span className="label">Δ platform − bank</span>
                  <span
                    className={`value mono ${
                      Math.abs(summary.summary.totals.platform_minus_bank) >
                      0.01
                        ? "bad"
                        : ""
                    }`}
                  >
                    {summary.summary.totals.platform_minus_bank.toLocaleString(
                      undefined,
                      { maximumFractionDigits: 2 }
                    )}
                  </span>
                </div>
                <div className="stat">
                  <span className="label">Payout vs bank (variance)</span>
                  <span className="value mono">
                    {summary.summary.totals.payout_vs_bank_settlement.toLocaleString(
                      undefined,
                      { maximumFractionDigits: 2 }
                    )}
                  </span>
                </div>
              </div>
            </div>
            <div className="card wide">
              <h3>Anomaly summary</h3>
              <div className="anomaly-types">
                {Object.entries(
                  summary.summary.anomaly_counts_by_type
                ).map(([t, n]) => (
                  <div key={t} className="type-chip">
                    <span className="mono">{n}</span>{" "}
                    <span>{t.replace(/_/g, " ")}</span>
                  </div>
                ))}
                {!Object.keys(summary.summary.anomaly_counts_by_type).length && (
                  <span className="muted">No anomalies detected</span>
                )}
              </div>
            </div>
          </section>
        )}

        {tab === "transactions" && platform && (
          <section>
            <h2>Platform transactions</h2>
            <p className="muted">
              Rows flagged when an anomaly references the same{" "}
              <code>transaction_id</code>.
            </p>
            {!platform.records.length ? (
              <p className="muted">No rows</p>
            ) : (
              <div className="table-wrap tall">
                <table>
                  <thead>
                    <tr>
                      {Object.keys(platform.records[0]).map((k) => (
                        <th key={k}>{k}</th>
                      ))}
                      <th>signals</th>
                    </tr>
                  </thead>
                  <tbody>
                    {txRowsFlagged.slice(0, 400).map((row, i) => {
                      const r = row as Record<string, unknown>;
                      const flags = (r._anomalies as Anomaly[]) ?? [];
                      const keys = Object.keys(platform.records[0]);
                      return (
                        <tr
                          key={i}
                          className={flags.length ? "row-flagged" : undefined}
                        >
                          {keys.map((k) => (
                            <td key={k} className="mono">
                              {formatCell(r[k])}
                            </td>
                          ))}
                          <td>
                            <div className="flex-wrap">
                              {flags.map((a, j) => (
                                <AnomalyBadge key={j} a={a} />
                              ))}
                              {!flags.length && "—"}
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        )}

        {tab === "settlements" && bank && (
          <section>
            <h2>Bank settlement logs</h2>
            <DataTable rows={bank.records} maxHeight="70vh" />
          </section>
        )}

        {tab === "payouts" && payouts && (
          <section>
            <h2>Merchant payouts</h2>
            <DataTable rows={payouts.records} maxHeight="50vh" />
          </section>
        )}

        {tab === "gaps" && summary && (
          <section className="gaps">
            <h2>Reconciliation gaps & anomalies</h2>
            <div className="card">
              <h3>All anomalies</h3>
              <ul className="anomaly-list">
                {summary.anomalies.map((a, i) => (
                  <li
                    key={i}
                    className="anomaly-item"
                    style={{ borderLeftColor: severityColor(a.severity) }}
                  >
                    <div className="anomaly-head">
                      <strong>{a.type?.replace(/_/g, " ")}</strong>
                      <AnomalyBadge a={a} />
                    </div>
                    <p className="detail">{String(a.detail ?? "")}</p>
                    {a.transaction_id != null && (
                      <p className="mono muted">tx: {String(a.transaction_id)}</p>
                    )}
                  </li>
                ))}
                {!summary.anomalies.length && (
                  <li className="muted">No anomalies</li>
                )}
              </ul>
            </div>
            <div className="three-col">
              <div className="card">
                <h3>Unmatched platform</h3>
                <DataTable
                  rows={summary.gaps.unmatched_platform}
                  maxHeight="40vh"
                />
              </div>
              <div className="card">
                <h3>Unmatched bank</h3>
                <DataTable rows={summary.gaps.unmatched_bank} maxHeight="40vh" />
              </div>
              <div className="card">
                <h3>Partial / amount mismatch</h3>
                <DataTable
                  rows={summary.gaps.partial_matches}
                  maxHeight="40vh"
                />
              </div>
            </div>
          </section>
        )}

        {tab === "monthly" && summary && (
          <section>
            <h2>Monthly report</h2>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Month</th>
                    <th>Platform rows</th>
                    <th>Platform amount</th>
                    <th>Matched in month</th>
                  </tr>
                </thead>
                <tbody>
                  {summary.monthly.map((m) => (
                    <tr key={m.month}>
                      <td className="mono">{m.month}</td>
                      <td>{m.platform_row_count}</td>
                      <td className="mono">
                        {m.platform_amount.toLocaleString(undefined, {
                          maximumFractionDigits: 2,
                        })}
                      </td>
                      <td>{m.matched_in_month}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}
      </main>
    </div>
  );
}
