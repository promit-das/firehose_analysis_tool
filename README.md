# Firehose Analysis Tool

Local metrics-first analyzer for Firehose `.txt` NDJSON exports.

## What it does
- Upload a local NDJSON `.txt` file from the web UI (ingestion step).
- Run analysis separately on an ingested run with analysis filters (`app_id`, `tenant_id`, `event_type`, time window, `delay_breach_ms`).
- Ingest into `events_raw` and normalize into `events_flat`.
- Compute deterministic metrics for:
  - event counts by event type + beacon
  - updates per minute per beacon
  - event-to-record delivery delay distributions and breach counts
- Generate run artifacts under `output/runs/<run_id>/`.
- Generate LLM markdown report and optional PDF.

## Stack
- Python 3.12+
- FastAPI + Jinja templates (server-rendered UI)
- DuckDB (metrics source of truth)

## Quickstart
1. Create a virtual environment and install dependencies:
   - `python3 -m venv .venv`
   - `source .venv/bin/activate`
   - `pip install -e .`
2. Create `.env` from `.env.example` and set LLM credentials.
3. Start app:
   - `uvicorn app.main:app --reload`
4. Open [http://127.0.0.1:8000](http://127.0.0.1:8000)

## Run outputs
Each run writes artifacts in `output/runs/<run_id>/`:
- `run_metadata.json`
- `metrics_summary.json`
- `metric_row_counts.json`
- `metrics_tables/*.csv`
- `ingest_errors.jsonl`
- report artifacts (`prompt_snapshot.txt`, `llm_input_payload.json`, `report.md`, optional `report.pdf`)

## Notes
- Startup validates mandatory LLM config and secrets by default (`LLM_REQUIRED_ON_STARTUP=1`).
- Set `LLM_REQUIRED_ON_STARTUP=0` to run ingest/metrics without report credentials.
- `.env.example` is configured for CIRCUIT by default (`LLM_PROVIDER=circuit` with `CIRCUIT_APP_KEY` plus either `CIRCUIT_API_KEY` or CIRCUIT OAuth vars).
- Extractor mappings are in `app/extractor_rules.json` and are intended to be calibrated against real sample data.
