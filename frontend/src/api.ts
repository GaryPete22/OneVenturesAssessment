const base = "";

export async function fetchJson<T>(path: string): Promise<T> {
  const r = await fetch(`${base}${path}`);
  if (!r.ok) throw new Error(`${path} ${r.status}`);
  return r.json() as Promise<T>;
}

export type Anomaly = {
  type: string;
  severity?: string;
  transaction_id?: string | null;
  detail?: string;
  [k: string]: unknown;
};

export type SummaryPayload = {
  summary: {
    counts: Record<string, number>;
    totals: Record<string, number>;
    anomaly_counts_by_type: Record<string, number>;
  };
  anomalies: Anomaly[];
  gaps: {
    unmatched_platform: Record<string, unknown>[];
    unmatched_bank: Record<string, unknown>[];
    partial_matches: Record<string, unknown>[];
  };
  matches_sample: Record<string, unknown>[];
  monthly: { month: string; platform_row_count: number; platform_amount: number; matched_in_month: number }[];
};

export type TableResponse = {
  records: Record<string, unknown>[];
  row_count: number;
};
