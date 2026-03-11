# Firehose Analysis Tool Plan (Python + FastAPI + DuckDB)

## Summary
Build in two milestones:
1. **M1 (first delivery):** deterministic pipeline only (UI file pick -> ingest -> normalize -> metric computation -> artifact export), fully test-verified.
2. **M2 (second delivery):** LLM report synthesis (`.md`) and optional `.pdf`, with prompt snapshotting and provider-agnostic env configuration.

This uses a local, server-rendered HTMX UI and DuckDB file storage as the metrics source of truth.

## Implementation Changes
1. **Project foundation**
- Create a Python 3.12 FastAPI app with server-rendered templates and HTMX actions.
- Establish three core modules in `app`, `sql`, and `prompts`.
- Add startup config validation (required env vars per provider) and fail-fast error messages.
- Add `.gitignore` to exclude `.env`, DuckDB database files, and generated run artifacts.

2. **M1 deterministic data pipeline**
- Implement streaming NDJSON parser (line-by-line, no full-file load), with per-line validation and error capture.
- Write valid rows to `events_raw(app_id, run_id, record_uid, tenant_id, event_type, record_ts_ms, payload_json)`.
- Enforce uniqueness `(app_id, run_id, record_uid)`; on duplicates, skip insert and increment duplicate counter in run stats.
- Normalize to `events_flat` via SQL transform rules plus event-type extractor mapping file (seeded from real sample data profiling).
- Compute required metric views/tables:
  - `metric_event_counts` (event_type + beacon_mac + time window counts)
  - `metric_updates_per_minute` (minute-bucket rates per beacon)
  - `metric_delivery_delay` (`record_ts_ms - event_ts_ms` distributions + breach counts)
- Persist run metadata and outputs under `output/runs/<run_id>/`:
  - `metrics_summary.json`
  - `metrics_tables/*.csv`
  - `run_metadata.json`
  - `ingest_errors.jsonl`

3. **M1 local UI workflow**
- `GET /`: ingestion-only form with file picker (`.txt`).
- `POST /runs`: ingest whole file only, then return run detail view.
- `POST /runs/{run_id}/analyze`: run metrics for selected `app_id` with optional analysis filters.
- `GET /runs/{run_id}`: show run status, analysis controls, key KPIs, anomaly highlights, and artifact links.
- Keep UX minimal and task-focused, no auth, local-only execution.

4. **M2 reporting layer**
- Add prompt composition: base prompt file + runtime “extra analysis instructions”.
- Add provider-agnostic LLM client config (`LLM_PROVIDER`, `LLM_MODEL`, `LLM_BASE_URL`, `LLM_TEMPERATURE`, provider key env var).
- Report input is metrics + sampled evidence only (no full raw corpus prompt).
- Persist reproducibility artifacts per run:
  - `prompt_snapshot.txt`
  - `llm_input_payload.json`
  - `report.md`
  - optional `report.pdf`
- Add `POST /runs/{run_id}/report` endpoint and UI action to generate report after metrics exist.

## Public Interfaces (Decision-Locked)
1. **HTTP endpoints**
- `GET /`
- `POST /runs` (multipart file + form fields)
- `POST /runs/{run_id}/analyze`
- `GET /runs/{run_id}`
- `GET /runs/{run_id}/artifacts/{filename}`
- `POST /runs/{run_id}/report` (M2)

2. **Core tables**
- `runs(run_id, app_id, source_filename, started_at, finished_at, status, total_lines, valid_lines, invalid_lines, duplicate_lines, config_json)`
- `ingest_errors(run_id, line_no, error_code, error_message, raw_line)`
- `events_raw(...)` and `events_flat(...)` with uniqueness `(app_id, run_id, record_uid)`
- `metric_*` materialized views/tables as above

3. **Metric defaults**
- Delay breach default: `30_000 ms` (user-overridable per run)

## Test Plan
1. **Unit tests**
- NDJSON line parsing, invalid JSON handling, timestamp parsing, deterministic `record_uid` fallback generation.
- Event timestamp extraction rules by event type.
- Metric SQL correctness for event counts, updates/minute, and delay distributions.

2. **Integration tests**
- End-to-end run on representative multi-tenant sample file with fixed expected outputs.
- Duplicate `record_uid` behavior and conflict handling.
- Mixed valid/invalid lines with accurate error accounting and run stats.

3. **API/UI tests**
- Form validation (`app_id` required, `.txt` input required).
- Run creation and artifact retrieval flows.
- M2: report generation path, prompt snapshot persistence, and missing-secret fail-fast behavior.

4. **Acceptance criteria**
- Same input + same run config yields identical metric artifacts.
- Required inferences are queryable and match golden expected values.
- LLM report generation never includes secrets and always stores prompt snapshot.

## Assumptions and Defaults
- Real sample NDJSON files will be supplied for extractor calibration before locking normalization mappings.
- Single-node local operation is in scope; no multi-user auth/session model in v1.
- All internal timestamps are stored/processed as epoch milliseconds in UTC.
- Vector/semantic layer remains disabled by default and out of M1 scope.
