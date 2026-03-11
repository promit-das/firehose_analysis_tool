from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from .artifacts import build_metrics_summary, run_output_dir
from .config import Settings
from .db import Database
from .pipeline import RunFilters, RunResult, analyze_existing_run, execute_run, ingest_only_run
from .reporting import generate_report_artifacts


class FirehoseService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.database = Database(
            db_path=settings.database_path,
            schema_path=settings.repo_root / "sql" / "schema.sql",
        )

    def initialize(self) -> None:
        self.settings.report_output_dir.mkdir(parents=True, exist_ok=True)
        self.database.init_schema()

    def run_file(
        self,
        file_path: Path,
        source_filename: str,
        app_id: str,
        filters: RunFilters,
    ) -> RunResult:
        with self.database.connect() as conn:
            result, _errors, _summary = execute_run(
                conn=conn,
                extractor_rules_path=self.settings.extractor_rules_path,
                output_base_dir=self.settings.report_output_dir,
                file_path=file_path,
                source_filename=source_filename,
                app_id=app_id,
                filters=filters,
            )
            return result

    def ingest_file(self, file_path: Path, source_filename: str) -> RunResult:
        with self.database.connect() as conn:
            result, _errors = ingest_only_run(
                conn=conn,
                output_base_dir=self.settings.report_output_dir,
                file_path=file_path,
                source_filename=source_filename,
                default_app_id=self.settings.default_app_id or "unknown_app",
            )
            return result

    def analyze_run(self, run_id: str, app_id: str, filters: RunFilters) -> dict[str, Any]:
        with self.database.connect() as conn:
            return analyze_existing_run(
                conn=conn,
                extractor_rules_path=self.settings.extractor_rules_path,
                output_base_dir=self.settings.report_output_dir,
                run_id=run_id,
                app_id=app_id,
                filters=filters,
            )

    def list_recent_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.database.connect() as conn:
            rows = conn.execute(
                """
                SELECT run_id, app_id, source_filename, started_at, finished_at, status,
                       total_lines, valid_lines, invalid_lines, duplicate_lines
                FROM runs
                ORDER BY started_at DESC
                LIMIT ?
                """,
                [limit],
            ).fetchall()

        return [
            {
                "run_id": row[0],
                "app_id": row[1],
                "source_filename": row[2],
                "started_at": row[3],
                "finished_at": row[4],
                "status": row[5],
                "total_lines": row[6],
                "valid_lines": row[7],
                "invalid_lines": row[8],
                "duplicate_lines": row[9],
            }
            for row in rows
        ]

    def get_run_context(self, run_id: str) -> dict[str, Any] | None:
        with self.database.connect() as conn:
            run_row = conn.execute(
                """
                SELECT run_id, app_id, source_filename, started_at, finished_at, status,
                       total_lines, valid_lines, invalid_lines, duplicate_lines, config_json, error_message
                FROM runs
                WHERE run_id = ?
                """,
                [run_id],
            ).fetchone()
            if run_row is None:
                return None

            summary = build_metrics_summary(conn, run_id)
            delay_preview = conn.execute(
                """
                SELECT tenant_id, event_type, beacon_mac, sample_count, p95_delay_ms, breach_count
                FROM metric_delivery_delay
                WHERE run_id = ?
                ORDER BY p95_delay_ms DESC NULLS LAST
                LIMIT 10
                """,
                [run_id],
            ).fetchall()
            available_app_ids = [
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT app_id FROM events_raw WHERE run_id = ? ORDER BY app_id",
                    [run_id],
                ).fetchall()
                if row[0] is not None
            ]

        output_dir = run_output_dir(self.settings.report_output_dir, run_id)
        artifacts = sorted(
            [path.relative_to(output_dir).as_posix() for path in output_dir.glob("**/*") if path.is_file()]
        )

        return {
            "run": {
                "run_id": run_row[0],
                "app_id": run_row[1],
                "source_filename": run_row[2],
                "started_at": run_row[3],
                "finished_at": run_row[4],
                "status": run_row[5],
                "total_lines": run_row[6],
                "valid_lines": run_row[7],
                "invalid_lines": run_row[8],
                "duplicate_lines": run_row[9],
                "config_json": run_row[10],
                "error_message": run_row[11],
            },
            "summary": summary,
            "delay_preview": [
                {
                    "tenant_id": row[0],
                    "event_type": row[1],
                    "beacon_mac": row[2],
                    "sample_count": row[3],
                    "p95_delay_ms": row[4],
                    "breach_count": row[5],
                }
                for row in delay_preview
            ],
            "available_app_ids": available_app_ids,
            "default_delay_breach_ms": self.settings.default_delay_breach_ms,
            "artifacts": artifacts,
        }

    def generate_report(self, run_id: str, extra_instructions: str | None, create_pdf: bool) -> dict[str, Any]:
        with self.database.connect() as conn:
            return generate_report_artifacts(
                conn=conn,
                settings=self.settings,
                run_id=run_id,
                extra_instructions=extra_instructions,
                create_pdf=create_pdf,
            )

    def default_filters(self) -> RunFilters:
        return RunFilters(
            tenant_id=self.settings.default_tenant_id,
            delay_breach_ms=self.settings.default_delay_breach_ms,
        )

    def output_dir_for_run(self, run_id: str) -> Path:
        return run_output_dir(self.settings.report_output_dir, run_id)

    @staticmethod
    def filters_from_form(
        tenant_id: str | None,
        event_type: str | None,
        start_ts_ms: str | None,
        end_ts_ms: str | None,
        delay_breach_ms: str | None,
        default_filters: RunFilters,
    ) -> RunFilters:
        def as_int(raw: str | None, fallback: int | None) -> int | None:
            if raw is None or not raw.strip():
                return fallback
            return int(raw.strip())

        return RunFilters(
            tenant_id=(tenant_id or "").strip() or default_filters.tenant_id,
            event_type=(event_type or "").strip() or None,
            start_ts_ms=as_int(start_ts_ms, None),
            end_ts_ms=as_int(end_ts_ms, None),
            delay_breach_ms=as_int(delay_breach_ms, default_filters.delay_breach_ms)
            or default_filters.delay_breach_ms,
        )

    @staticmethod
    def run_result_to_dict(result: RunResult) -> dict[str, Any]:
        return {
            "run_id": result.run_id,
            "app_id": result.app_id,
            "source_filename": result.source_filename,
            "status": result.status,
            "ingest_stats": asdict(result.ingest_stats),
        }
