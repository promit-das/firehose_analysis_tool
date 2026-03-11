from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def run_output_dir(base_output_dir: Path, run_id: str) -> Path:
    path = base_output_dir / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def _query_rows(conn, query: str, params: list[Any]) -> tuple[list[str], list[tuple[Any, ...]]]:
    result = conn.execute(query, params)
    names = [item[0] for item in result.description]
    rows = result.fetchall()
    return names, rows


def write_query_csv(conn, query: str, params: list[Any], target_path: Path) -> int:
    columns, rows = _query_rows(conn, query, params)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)
    return len(rows)


def metric_table_names() -> list[str]:
    return [
        "metric_event_counts",
        "metric_updates_per_minute",
        "metric_delivery_delay",
    ]


def export_metric_tables(conn, run_id: str, output_dir: Path) -> dict[str, int]:
    metrics_dir = output_dir / "metrics_tables"
    counts: dict[str, int] = {}
    for table_name in metric_table_names():
        row_count = write_query_csv(
            conn,
            f"SELECT * FROM {table_name} WHERE run_id = ?",
            [run_id],
            metrics_dir / f"{table_name}.csv",
        )
        counts[table_name] = row_count
    return counts


def build_metrics_summary(conn, run_id: str) -> dict[str, Any]:
    summary_row = conn.execute(
        """
        SELECT
          run_id,
          COUNT(*) AS flat_event_count,
          COUNT(DISTINCT beacon_mac) FILTER (WHERE beacon_mac IS NOT NULL) AS distinct_beacons,
          COUNT(DISTINCT tenant_id) FILTER (WHERE tenant_id IS NOT NULL) AS distinct_tenants
        FROM events_flat
        WHERE run_id = ?
        GROUP BY run_id
        """,
        [run_id],
    ).fetchone()

    delivery_row = conn.execute(
        """
        SELECT
          COALESCE(SUM(sample_count), 0) AS delivery_samples,
          COALESCE(SUM(breach_count), 0) AS delivery_breaches,
          COALESCE(MAX(p95_delay_ms), 0) AS worst_p95_delay_ms
        FROM metric_delivery_delay
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()

    if summary_row is None:
        return {
            "run_id": run_id,
            "flat_event_count": 0,
            "distinct_beacons": 0,
            "distinct_tenants": 0,
            "delivery_samples": 0,
            "delivery_breaches": 0,
            "worst_p95_delay_ms": 0,
        }

    return {
        "run_id": summary_row[0],
        "flat_event_count": int(summary_row[1]),
        "distinct_beacons": int(summary_row[2]),
        "distinct_tenants": int(summary_row[3]),
        "delivery_samples": int(delivery_row[0]),
        "delivery_breaches": int(delivery_row[1]),
        "worst_p95_delay_ms": float(delivery_row[2]),
    }
