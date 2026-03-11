# Firehose Analysis Tool Project

## Business Requirements

- Build a production-ready Firehose analysis tool for raw `.txt` files containing NDJSON records.
- The input file can contain multiple tenant IDs; `app_id` is the primary scope key.
- Provide a local UI that lets the user select a downloaded `.txt` file from local disk.
- Ingest the selected file into a metrics-first datastore.
- Compute deterministic inferences for telemetry quality and delivery behavior.
- Generate a report using an LLM as `.md` and optionally `.pdf`.
- Prompt instructions must be extendable via a base prompt file and runtime chat/UI additions.

## Technical Details

- Use a metrics-first architecture: SQL aggregation is the source of truth.
- Keep vector search optional and secondary for exploratory semantic Q&A only.
- Core data flow:
1. UI file selection (`.txt`)
2. NDJSON ingest into `events_raw`
3. Normalization into `events_flat`
4. Metric computation (`metric_*` views/tables)
5. LLM report synthesis from metrics and evidence
- Required core entities:
- `events_raw(app_id, run_id, record_uid, tenant_id, event_type, record_ts_ms, payload_json)`
- `events_flat(app_id, run_id, record_uid, tenant_id, event_type, beacon_mac, event_ts_ms, record_ts_ms, location_id, ...)`
- Primary uniqueness scope: `(app_id, run_id, record_uid)`

## Required Inferences

- Count occurrences for event type + beacon MAC within a specified time window.
- Compute updates per minute per beacon.
- Compute event-to-record delivery delay (e.g., button press event timestamp vs firehose record timestamp), including distribution metrics and breach counts.

## LLM and Reporting Requirements

- LLM must be configurable by environment variables (provider/model/base URL/temperature).
- API keys must come from environment variables only; never hardcode secrets.
- LLM should analyze computed metrics and sampled evidence, not full raw rows.
- Reports must be reproducible:
- Persist prompt snapshot used for generation.
- Persist run metadata and non-sensitive context.
- Required outputs per run:
- Markdown report (`.md`)
- Optional PDF export (`.pdf`)

## Configuration and Secrets

- Use repo-local `.env` for runtime configuration.
- Keep `.env.example` committed as the template for required variables.
- Keep `.env` out of version control.
- Fail fast with clear startup errors when mandatory secrets/config are missing.

## Strategy

1. Start with ingestion + normalization + metric computation only; validate correctness first.
2. Add LLM reporting after deterministic metrics are stable and verifiable.
3. Keep the UI minimal and task-focused: file pick, run inputs, run action, output links.
4. Do not add features outside agreed scope unless explicitly approved.

## Coding Standards

1. Keep implementation simple and pragmatic; avoid over-engineering.
2. Prioritize correctness, traceability, and deterministic outputs.
3. Separate pure metric logic from LLM narrative logic.
4. Keep prompts versioned and reviewable.
5. Never expose secrets in logs, reports, or committed files.
6. No emojis in code comments, docs, or commit messages.

## Non-Goals (Current Scope)

- No vectorization of all raw events by default.
- No dependence on cross-run vector continuity.
- No direct row-by-row LLM analysis across the entire raw corpus.
