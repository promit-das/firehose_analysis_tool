# Firehose Analysis Tool - Design Document

## 1. Objective
Build a production-ready analysis tool for Firehose raw output files that:
- Ingests `.txt` files containing newline-delimited JSON events.
- Supports files containing multiple tenant IDs.
- Treats `app_id` as the primary scope key.
- Produces deterministic operational metrics.
- Uses an LLM to generate human-readable reports (`.md` and optional `.pdf`) from computed metrics and evidence.

This document is design-only. No implementation details are executed in this phase.

## 2. Key Decisions
- Data strategy: metrics-first (SQL-first), not vector-first.
- Primary scope key: `app_id`.
- Multi-tenant model: a single input file may include many `tenant_id` values.
- Input format: `.txt` file (NDJSON content).
- UI requirement: user must be able to select a local downloaded `.txt` file.
- LLM role: narrative synthesis and prioritization over validated metric outputs.
- Optional semantic layer: vectors are secondary and only for exploratory question answering.

## 3. Functional Requirements
1. Accept `.txt` input file from local directory via UI file picker.
2. Capture `app_id` as required run input.
3. Ingest file records into a metrics-first datastore.
4. Normalize raw events into queryable fields.
5. Support KPI/inference queries including:
- Event occurrence counts for event type + beacon MAC + time window.
- Updates-per-minute per beacon.
- Event-to-record delivery delay (e.g., button press event timestamp vs firehose record timestamp).
6. Generate an output analysis report as `.md` and optionally `.pdf`.
7. Allow prompt updates from:
- A maintained prompt file.
- Runtime user/chat-provided additional analysis points.

## 4. Data Characteristics and Implications
- Input is high-volume raw telemetry (not narrative logs).
- Records are deeply nested JSON payloads.
- Single file can contain mixed tenants.
- High event volume makes row-by-row LLM analysis impractical.

Implication:
- Deterministic SQL aggregation must be the source of truth.
- LLM consumes summarized metrics and evidence, not the full raw dataset.

## 5. Proposed Architecture
### 5.1 UI Layer
- Local web UI with:
- `.txt` file picker.
- Required `app_id` field.
- Optional filters (`tenant_id`, time window, event type).
- Optional “extra analysis instructions” text box.
- Action buttons for ingest and report generation.

### 5.2 Ingestion Layer
- Input `.txt` parsed as NDJSON.
- Record-level validation and error accounting.
- Add run metadata: `run_id`, `app_id`, ingestion timestamp.
- Persist raw events into datastore.

### 5.3 Normalization Layer
- Flatten critical fields from nested JSON into a normalized events table.
- Maintain event-type-specific timestamp extraction rules to derive canonical `event_ts_ms`.
- Preserve `record_ts_ms` for delivery delay calculations.

### 5.4 Metrics Layer (Primary Analytics Engine)
- Materialized metrics/views for fast repeatable analysis:
- Event count metrics by dimensions.
- Per-minute update rates.
- Delivery delay distributions and SLA breach counters.

### 5.5 LLM Report Layer
- Input to LLM:
- Structured KPI payload.
- Top anomalies.
- Representative evidence rows/snippets.
- Prompt composition:
- Base prompt template from file.
- Runtime extra prompt instructions from UI/chat.
- Output:
- Markdown report.
- Optional PDF rendered from markdown.

### 5.6 Optional Semantic Exploration Layer
- If enabled, create temporary vector index from anomaly summaries only.
- Do not vectorize full raw event corpus by default.
- Use only for ad hoc semantic Q&A.

## 6. Data Model (Logical)
### 6.1 `events_raw`
- `app_id`
- `run_id`
- `record_uid`
- `tenant_id`
- `event_type`
- `record_ts_ms`
- `payload_json`

Logical uniqueness:
- `(app_id, run_id, record_uid)`

### 6.2 `events_flat`
- `app_id`
- `run_id`
- `record_uid`
- `tenant_id`
- `event_type`
- `beacon_mac`
- `event_ts_ms`
- `record_ts_ms`
- `location_id`
- additional normalized fields as needed

Logical uniqueness:
- `(app_id, run_id, record_uid)`

### 6.3 `metric_*` tables/views
- `metric_event_counts`
- `metric_updates_per_minute`
- `metric_delivery_delay`

## 7. Query and Inference Mapping
1. Event count for event type + MAC + duration:
- Filter by `app_id`, optional `tenant_id`, `event_type`, `beacon_mac`, time bounds.

2. Updates-per-minute per beacon:
- Group by minute bucket of timestamp and `beacon_mac`.

3. Event-to-record delay (e.g., button press):
- `delay_ms = record_ts_ms - event_ts_ms`.
- Compute p50/p95/max and threshold breach counts.

## 8. Prompting and Report Governance
- Keep base prompt versioned in repository.
- Capture per-run prompt additions from UI/chat.
- Persist final prompt used for each report for auditability.
- Require evidence-backed report sections.
- Require explicit “insufficient evidence” statements when data is missing.

## 9. Output Artifacts
Per run:
- Structured metric outputs.
- Markdown report (`.md`).
- Optional PDF report (`.pdf`).
- Prompt snapshot and run metadata.

## 10. Performance Strategy
- Prioritize SQL aggregation over full-corpus embeddings.
- Normalize only required fields for target inferences.
- Batch ingestion/parsing.
- Use pre-aggregated metric tables for LLM input.
- Use optional vectors only for exploratory semantic use cases.

## 11. Non-Goals (Current Phase)
- No cross-run continuity requirement for vector index.
- No requirement to keep a long-lived per-tenant vector history.
- No direct LLM analysis over every raw input row.

## 12. Implementation Guardrail for Next Phase
When implementation begins (outside this design step):
- Start with ingestion + normalization + metrics computation.
- Add LLM report generation after deterministic metric outputs are validated.
- Keep UI minimal and task-focused.

## 13. Configuration & Secrets
### 13.1 LLM Configuration (Runtime)
The tool must support runtime-configurable LLM settings:
- `LLM_PROVIDER` (e.g., `circuit`, `openai`, `azure_openai`, `anthropic`, or compatible gateway)
- `LLM_MODEL` (exact model identifier)
- `LLM_BASE_URL` (optional, for proxy/gateway/self-hosted-compatible endpoints)
- `LLM_TEMPERATURE` (optional; default `0` for deterministic reporting)

Design rule:
- Provider/model must be configurable without code changes.
- Prompt/report logic must remain provider-agnostic.

### 13.2 API Keys and Secret Variables
Secrets must be provided via environment variables, never hardcoded.
Expected variables depend on provider, for example:
- CIRCUIT: `CIRCUIT_API_KEY` and `CIRCUIT_APP_KEY` (or OAuth token settings `CIRCUIT_API_CLIENT_ID`, `CIRCUIT_API_CLIENT_SECRET`, `CIRCUIT_API_URL` with `CIRCUIT_APP_KEY`)
- OpenAI-compatible: `OPENAI_API_KEY`
- Azure OpenAI: `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`
- Anthropic: `ANTHROPIC_API_KEY`

Optional non-secret runtime vars:
- `DEFAULT_APP_ID`
- `DEFAULT_TENANT_ID`
- `REPORT_OUTPUT_DIR`

### 13.3 `.env` File Strategy
Recommended approach:
- Maintain a dedicated `.env` in `/Users/promit/Projects/firehose_analysis_tool`.
- Maintain a committed `.env.example` documenting required variables.

### 13.4 Security and Operational Controls
- Never commit real keys or tokens.
- Add `.env` to `.gitignore`.
- Mask keys/tokens in logs and UI messages.
- Validate required secrets at startup and fail fast with clear missing-variable errors.
- Persist only non-sensitive run metadata in report artifacts.
