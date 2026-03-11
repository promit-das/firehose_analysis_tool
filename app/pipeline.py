from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

from .artifacts import build_metrics_summary, export_metric_tables, run_output_dir, write_json, write_jsonl
from .extractors import extract_flat_record, load_extractor_rules, parse_raw_record


@dataclass(frozen=True)
class RunFilters:
    tenant_id: str | None = None
    event_type: str | None = None
    start_ts_ms: int | None = None
    end_ts_ms: int | None = None
    delay_breach_ms: int = 30000


@dataclass(frozen=True)
class IngestStats:
    total_lines: int
    valid_lines: int
    invalid_lines: int
    duplicate_lines: int


@dataclass(frozen=True)
class RunResult:
    run_id: str
    app_id: str
    source_filename: str
    status: str
    ingest_stats: IngestStats


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _insert_run_start(conn, run_id: str, app_id: str, source_filename: str, config_json: str) -> None:
    conn.execute(
        """
        INSERT INTO runs (run_id, app_id, source_filename, started_at, status, config_json)
        VALUES (?, ?, ?, ?, 'RUNNING', ?)
        """,
        [run_id, app_id, source_filename, _utc_now(), config_json],
    )


def _mark_run_failed(conn, run_id: str, message: str) -> None:
    conn.execute(
        """
        UPDATE runs
        SET status = 'FAILED', finished_at = ?, error_message = ?
        WHERE run_id = ?
        """,
        [_utc_now(), message[:2000], run_id],
    )


def _mark_run_finished(conn, run_id: str, stats: IngestStats, status: str = "COMPLETED") -> None:
    conn.execute(
        """
        UPDATE runs
        SET status = ?,
            finished_at = ?,
            total_lines = ?,
            valid_lines = ?,
            invalid_lines = ?,
            duplicate_lines = ?
        WHERE run_id = ?
        """,
        [
            status,
            _utc_now(),
            stats.total_lines,
            stats.valid_lines,
            stats.invalid_lines,
            stats.duplicate_lines,
            run_id,
        ],
    )


def ingest_ndjson_file(
    conn,
    file_path: Path,
    run_id: str,
    default_app_id: str,
) -> tuple[IngestStats, list[dict[str, Any]]]:
    total_lines = 0
    valid_lines = 0
    invalid_lines = 0
    duplicate_lines = 0
    errors: list[dict[str, Any]] = []

    with file_path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            stripped = raw_line.strip()
            if not stripped:
                continue

            total_lines += 1

            try:
                payload = json.loads(stripped)
                if not isinstance(payload, dict):
                    raise ValueError("Record must be a JSON object")
                parsed = parse_raw_record(payload, stripped, default_app_id=default_app_id)
                conn.execute(
                    """
                    INSERT INTO events_raw (
                      app_id,
                      run_id,
                      record_uid,
                      tenant_id,
                      event_type,
                      record_ts_ms,
                      payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        parsed["app_id"],
                        run_id,
                        parsed["record_uid"],
                        parsed["tenant_id"],
                        parsed["event_type"],
                        parsed["record_ts_ms"],
                        parsed["payload_json"],
                    ],
                )
                valid_lines += 1
            except Exception as exc:
                message = str(exc)
                if "duplicate key" in message.lower() or "constraint" in message.lower():
                    duplicate_lines += 1
                    continue

                invalid_lines += 1
                errors.append(
                    {
                        "run_id": run_id,
                        "line_no": line_no,
                        "error_code": "invalid_record",
                        "error_message": message,
                        "raw_line": stripped[:4000],
                    }
                )

    if errors:
        conn.executemany(
            """
            INSERT INTO ingest_errors (run_id, line_no, error_code, error_message, raw_line)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                [
                    err["run_id"],
                    err["line_no"],
                    err["error_code"],
                    err["error_message"],
                    err["raw_line"],
                ]
                for err in errors
            ],
        )

    return (
        IngestStats(
            total_lines=total_lines,
            valid_lines=valid_lines,
            invalid_lines=invalid_lines,
            duplicate_lines=duplicate_lines,
        ),
        errors,
    )


def normalize_events(conn, app_id: str, run_id: str, extractor_rules: dict[str, Any]) -> int:
    rows = conn.execute(
        """
        SELECT record_uid, tenant_id, event_type, record_ts_ms, payload_json
        FROM events_raw
        WHERE app_id = ? AND run_id = ?
        """,
        [app_id, run_id],
    ).fetchall()

    inserted = 0
    for record_uid, tenant_id, event_type, record_ts_ms, payload_json in rows:
        payload = json.loads(payload_json)
        flat = extract_flat_record(payload, event_type, record_ts_ms, extractor_rules)
        conn.execute(
            """
            INSERT INTO events_flat (
              app_id,
              run_id,
              record_uid,
              tenant_id,
              event_type,
              beacon_mac,
              event_ts_ms,
              record_ts_ms,
              location_id,
              payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                app_id,
                run_id,
                record_uid,
                tenant_id,
                event_type,
                flat["beacon_mac"],
                flat["event_ts_ms"],
                record_ts_ms,
                flat["location_id"],
                payload_json,
            ],
        )
        inserted += 1

    return inserted


def _where_clause(filters: RunFilters) -> tuple[str, list[Any]]:
    parts: list[str] = ["app_id = ?", "run_id = ?", "event_ts_ms IS NOT NULL"]
    params: list[Any] = []

    if filters.tenant_id:
        parts.append("tenant_id = ?")
        params.append(filters.tenant_id)
    if filters.event_type:
        parts.append("event_type = ?")
        params.append(filters.event_type)
    if filters.start_ts_ms is not None:
        parts.append("event_ts_ms >= ?")
        params.append(filters.start_ts_ms)
    if filters.end_ts_ms is not None:
        parts.append("event_ts_ms <= ?")
        params.append(filters.end_ts_ms)

    return " AND ".join(parts), params


def compute_metrics(conn, app_id: str, run_id: str, filters: RunFilters) -> None:
    for table_name in [
        "metric_event_counts",
        "metric_updates_per_minute",
        "metric_delivery_delay",
    ]:
        conn.execute(f"DELETE FROM {table_name} WHERE run_id = ?", [run_id])

    where_sql, filter_values = _where_clause(filters)

    base_values = [app_id, run_id] + filter_values

    conn.execute(
        f"""
        INSERT INTO metric_event_counts
        SELECT
          app_id,
          run_id,
          tenant_id,
          event_type,
          beacon_mac,
          MIN(event_ts_ms) AS window_start_ms,
          MAX(event_ts_ms) AS window_end_ms,
          COUNT(*) AS event_count
        FROM events_flat
        WHERE {where_sql}
        GROUP BY app_id, run_id, tenant_id, event_type, beacon_mac
        """,
        base_values,
    )

    conn.execute(
        f"""
        INSERT INTO metric_updates_per_minute
        SELECT
          app_id,
          run_id,
          tenant_id,
          beacon_mac,
          DATE_TRUNC('minute', TO_TIMESTAMP(event_ts_ms / 1000.0)) AS minute_ts,
          COUNT(*) AS updates_count
        FROM events_flat
        WHERE {where_sql}
        GROUP BY app_id, run_id, tenant_id, beacon_mac, minute_ts
        """,
        base_values,
    )

    delivery_values = base_values + [filters.delay_breach_ms, filters.delay_breach_ms]
    conn.execute(
        f"""
        INSERT INTO metric_delivery_delay
        WITH scoped AS (
          SELECT
            app_id,
            run_id,
            tenant_id,
            event_type,
            beacon_mac,
            record_ts_ms - event_ts_ms AS delay_ms
          FROM events_flat
          WHERE {where_sql} AND record_ts_ms IS NOT NULL
        )
        SELECT
          app_id,
          run_id,
          tenant_id,
          event_type,
          beacon_mac,
          COUNT(*) AS sample_count,
          QUANTILE_CONT(delay_ms, 0.5) AS p50_delay_ms,
          QUANTILE_CONT(delay_ms, 0.95) AS p95_delay_ms,
          AVG(delay_ms) AS avg_delay_ms,
          MAX(delay_ms) AS max_delay_ms,
          SUM(CASE WHEN delay_ms > ? THEN 1 ELSE 0 END) AS breach_count,
          SUM(CASE WHEN delay_ms < 0 THEN 1 ELSE 0 END) AS negative_delay_count,
          ? AS threshold_ms
        FROM scoped
        GROUP BY app_id, run_id, tenant_id, event_type, beacon_mac
        """,
        delivery_values,
    )


def _write_run_artifacts(
    conn,
    output_base_dir: Path,
    run_id: str,
    metadata: dict[str, Any],
    ingest_errors: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, int]]:
    out_dir = run_output_dir(output_base_dir, run_id)
    summary = build_metrics_summary(conn, run_id)
    metric_counts = export_metric_tables(conn, run_id, out_dir)
    write_json(out_dir / "run_metadata.json", metadata)
    write_json(out_dir / "metrics_summary.json", summary)
    write_json(out_dir / "metric_row_counts.json", metric_counts)
    write_jsonl(out_dir / "ingest_errors.jsonl", ingest_errors)
    return summary, metric_counts


def ingest_only_run(
    conn,
    output_base_dir: Path,
    file_path: Path,
    source_filename: str,
    default_app_id: str,
) -> tuple[RunResult, list[dict[str, Any]]]:
    run_id = str(uuid4())
    ingest_scope_app_id = default_app_id
    _insert_run_start(conn, run_id, ingest_scope_app_id, source_filename, json.dumps({"mode": "ingest_only"}))

    try:
        ingest_stats, ingest_errors = ingest_ndjson_file(
            conn=conn,
            file_path=file_path,
            run_id=run_id,
            default_app_id=default_app_id,
        )
        _mark_run_finished(conn, run_id, ingest_stats, status="INGESTED")

        available_app_ids = [
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT app_id FROM events_raw WHERE run_id = ? ORDER BY app_id",
                [run_id],
            ).fetchall()
            if row[0] is not None
        ]

        metadata = {
            "run_id": run_id,
            "app_id": ingest_scope_app_id,
            "source_filename": source_filename,
            "status": "INGESTED",
            "mode": "ingest_only",
            "ingest_stats": asdict(ingest_stats),
            "available_app_ids": available_app_ids,
            "generated_at": _utc_now().isoformat(),
        }
        out_dir = run_output_dir(output_base_dir, run_id)
        write_json(out_dir / "run_metadata.json", metadata)
        write_jsonl(out_dir / "ingest_errors.jsonl", ingest_errors)

        return (
            RunResult(
                run_id=run_id,
                app_id=ingest_scope_app_id,
                source_filename=source_filename,
                status="INGESTED",
                ingest_stats=ingest_stats,
            ),
            ingest_errors,
        )
    except Exception:
        _mark_run_failed(conn, run_id, "Ingestion failed; inspect server logs for details")
        raise


def analyze_existing_run(
    conn,
    extractor_rules_path: Path,
    output_base_dir: Path,
    run_id: str,
    app_id: str,
    filters: RunFilters,
) -> dict[str, Any]:
    run_row = conn.execute(
        """
        SELECT source_filename, total_lines, valid_lines, invalid_lines, duplicate_lines
        FROM runs
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()
    if run_row is None:
        raise ValueError(f"Run {run_id} not found")

    conn.execute("DELETE FROM events_flat WHERE run_id = ?", [run_id])

    extractor_rules = load_extractor_rules(extractor_rules_path)
    inserted_rows = normalize_events(conn, app_id, run_id, extractor_rules)
    if inserted_rows == 0:
        raise ValueError(f"No rows found for app_id={app_id} in run {run_id}")

    compute_metrics(conn, app_id, run_id, filters)

    conn.execute(
        """
        UPDATE runs
        SET status = 'COMPLETED',
            app_id = ?,
            config_json = ?,
            finished_at = ?
        WHERE run_id = ?
        """,
        [app_id, json.dumps(asdict(filters), sort_keys=True), _utc_now(), run_id],
    )

    ingest_errors = conn.execute(
        """
        SELECT run_id, line_no, error_code, error_message, raw_line
        FROM ingest_errors
        WHERE run_id = ?
        ORDER BY line_no
        """,
        [run_id],
    ).fetchall()
    ingest_errors_payload = [
        {
            "run_id": row[0],
            "line_no": row[1],
            "error_code": row[2],
            "error_message": row[3],
            "raw_line": row[4],
        }
        for row in ingest_errors
    ]

    metadata = {
        "run_id": run_id,
        "app_id": app_id,
        "source_filename": run_row[0],
        "status": "COMPLETED",
        "mode": "analyzed",
        "filters": asdict(filters),
        "ingest_stats": {
            "total_lines": int(run_row[1]),
            "valid_lines": int(run_row[2]),
            "invalid_lines": int(run_row[3]),
            "duplicate_lines": int(run_row[4]),
        },
        "normalized_rows": inserted_rows,
        "generated_at": _utc_now().isoformat(),
    }

    summary, metric_counts = _write_run_artifacts(
        conn=conn,
        output_base_dir=output_base_dir,
        run_id=run_id,
        metadata=metadata,
        ingest_errors=ingest_errors_payload,
    )

    return {
        "run_id": run_id,
        "app_id": app_id,
        "summary": summary,
        "metric_row_counts": metric_counts,
    }


def execute_run(
    conn,
    extractor_rules_path: Path,
    output_base_dir: Path,
    file_path: Path,
    source_filename: str,
    app_id: str,
    filters: RunFilters,
) -> tuple[RunResult, list[dict[str, Any]], dict[str, Any]]:
    run_id = str(uuid4())
    config_json = json.dumps(asdict(filters), sort_keys=True)
    _insert_run_start(conn, run_id, app_id, source_filename, config_json)

    try:
        ingest_stats, ingest_errors = ingest_ndjson_file(
            conn=conn,
            file_path=file_path,
            run_id=run_id,
            default_app_id=app_id,
        )
        extractor_rules = load_extractor_rules(extractor_rules_path)
        normalize_events(conn, app_id, run_id, extractor_rules)
        compute_metrics(conn, app_id, run_id, filters)
        _mark_run_finished(conn, run_id, ingest_stats)

        metadata = {
            "run_id": run_id,
            "app_id": app_id,
            "source_filename": source_filename,
            "status": "COMPLETED",
            "filters": asdict(filters),
            "ingest_stats": asdict(ingest_stats),
            "generated_at": _utc_now().isoformat(),
        }
        summary, _metric_counts = _write_run_artifacts(
            conn=conn,
            output_base_dir=output_base_dir,
            run_id=run_id,
            metadata=metadata,
            ingest_errors=ingest_errors,
        )

        return (
            RunResult(
                run_id=run_id,
                app_id=app_id,
                source_filename=source_filename,
                status="COMPLETED",
                ingest_stats=ingest_stats,
            ),
            ingest_errors,
            summary,
        )
    except Exception:
        _mark_run_failed(conn, run_id, "Pipeline execution failed; inspect server logs for details")
        raise
