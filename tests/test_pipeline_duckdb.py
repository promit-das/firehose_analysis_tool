from __future__ import annotations

from pathlib import Path
import tempfile

import pytest

from app.db import Database
from app.pipeline import RunFilters, execute_run


duckdb = pytest.importorskip("duckdb")


def _write_sample(path: Path) -> None:
    rows = [
        '{"record_uid":"r1","tenant_id":"t1","event_type":"button_press","record_ts_ms":1700000010000,"beacon":{"mac":"B1"},"button":{"ts_ms":1700000000000}}',
        '{"record_uid":"r2","tenant_id":"t1","event_type":"button_press","record_ts_ms":1700000021000,"beacon":{"mac":"B1"},"button":{"ts_ms":1700000010000}}',
        '{"record_uid":"r3","tenant_id":"t1","event_type":"button_press","record_ts_ms":1700000045000,"beacon":{"mac":"B1"},"button":{"ts_ms":1700000023000}}'
    ]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def test_execute_run_computes_metrics() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        temp = Path(tempdir)
        db_path = temp / "test.duckdb"
        output_dir = temp / "out"
        sample = temp / "sample.txt"
        _write_sample(sample)

        db = Database(db_path=db_path, schema_path=Path("sql/schema.sql"))
        db.init_schema()

        with db.connect() as conn:
            result, errors, summary = execute_run(
                conn=conn,
                extractor_rules_path=Path("app/extractor_rules.json"),
                output_base_dir=output_dir,
                file_path=sample,
                source_filename="sample.txt",
                app_id="app-a",
                filters=RunFilters(delay_breach_ms=30000),
            )

            assert result.status == "COMPLETED"
            assert not errors
            assert summary["flat_event_count"] == 3

            delay = conn.execute(
                "SELECT SUM(breach_count) FROM metric_delivery_delay WHERE run_id = ?",
                [result.run_id],
            ).fetchone()
            assert delay is not None
            assert delay[0] == 0

            updates = conn.execute(
                "SELECT COUNT(*) FROM metric_updates_per_minute WHERE run_id = ?",
                [result.run_id],
            ).fetchone()
            assert updates is not None
            assert updates[0] > 0
