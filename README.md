# OneVentures — Payments reconciliation

Name- Gary Rodriques
MITADT



Full-stack reconciliation for a payments platform: ingest **platform transactions**, **bank settlements**, and **merchant payouts**, normalize schemas, match records, flag anomalies, and review results in a dashboard.

## What it does

- **Match pipeline:** exact match on `transaction_id`, then fallback match on amount (within tolerance) and settlement date window (T+1–T+3 calendar days vs platform `created_at`).
- **Anomalies:** duplicate bank rows per `transaction_id`, settlement in a later calendar month, small aggregate total drift (rounding band), refunds without a matching non-refund platform row.
- **Reporting:** matched vs unmatched counts, platform vs bank totals, payout vs settlement variance, monthly rollups.

## Repository layout

| Path | Purpose |
|------|---------|
| `backend/` | FastAPI app, pandas pipeline, pytest suite |
| `frontend/` | React + Vite dashboard |
| `platform_transactions.csv` | Sample platform extract |
| `bank_settlements.csv` | Sample bank extract |
| `payouts.csv` | Sample payouts |

## Prerequisites

- Python 3.10+ (3.14 used in CI/dev is fine)
- Node.js 18+ (for the UI)

## Backend

```bash
cd backend
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

By default CSVs are read from the **repository root** (parent of `backend/`). Override with environment variables (prefix `RECON_`):

| Variable | Description | Default |
|----------|-------------|---------|
| `RECON_DATA_DIR` | Folder containing the three CSV files | Repo root |
| `RECON_AMOUNT_TOLERANCE` | Max \|platform − bank\| for fallback match | `0.01` |
| `RECON_SETTLEMENT_WINDOW_MIN_DAYS` | Min days from txn date to `settled_at` | `1` |
| `RECON_SETTLEMENT_WINDOW_MAX_DAYS` | Max days from txn date to `settled_at` | `3` |
| `RECON_ROUNDING_AGGREGATE_THRESHOLD` | Flag aggregate drift if 0 < \|diff\| < this | `1.0` |

### API (JSON)

- `GET /api/health`
- `GET /api/platform-transactions` — normalized platform rows
- `GET /api/bank-settlements` — normalized bank rows
- `GET /api/payouts` — normalized payouts
- `GET /api/reconciliation/summary` — summary, anomalies, gaps, sample matches, monthly report
- `GET /api/reconciliation/matches` — full match list
- `POST /api/ingest` — multipart upload: optional files `platform_transactions`, `bank_settlements`, `payouts`
- `POST /api/reload-default` — reload CSVs from disk; optional query `data_dir=...`

## Frontend

```bash
cd frontend
npm install
npm run dev
```

Open [http://localhost:5173](http://localhost:5173). The dev server proxies `/api` to `http://127.0.0.1:8000`, so keep the backend running.

Production build:

```bash
npm run build
```

Serve `frontend/dist/` behind any static host and point API calls to your deployed backend (or configure a reverse proxy for `/api`).

## Tests

```bash
cd backend
python -m pytest tests -v
```

- `tests/test_anomalies.py` — one scenario per anomaly type.
- `tests/test_reconcile_edges.py` — exact match, fallback, partial amount mismatch, unmatched when id/window don’t align, duplicate platform rows vs one bank row.

## Data notes

Some exports leave `amount` empty and put the monetary value in `user_id`. Normalization coalesces that for reconciliation and sets `amount_inferred_from_user_id` where applicable.

## License

Use and modify as needed for your assessment or internal use.
