from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from .artifacts import run_output_dir, write_json
from .config import Settings
from .llm_client import generate_report_text


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_base_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _compose_prompts(base_prompt: str, extra_instructions: str | None) -> tuple[str, str]:
    system_prompt = base_prompt
    user_prompt = "Generate a deterministic evidence-backed report from the payload."
    if extra_instructions and extra_instructions.strip():
        user_prompt += "\n\nAdditional runtime instructions:\n" + extra_instructions.strip()
    return system_prompt, user_prompt


def _build_report_payload(conn, run_id: str) -> dict[str, Any]:
    run_row = conn.execute(
        """
        SELECT run_id, app_id, source_filename, status, total_lines, valid_lines, invalid_lines, duplicate_lines
        FROM runs
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()
    if run_row is None:
        raise ValueError(f"Run {run_id} not found")

    summary_row = conn.execute(
        """
        SELECT
          COUNT(*) AS flat_events,
          COUNT(DISTINCT beacon_mac) FILTER (WHERE beacon_mac IS NOT NULL) AS beacons,
          COUNT(DISTINCT tenant_id) FILTER (WHERE tenant_id IS NOT NULL) AS tenants
        FROM events_flat
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()

    top_delivery = conn.execute(
        """
        SELECT tenant_id, event_type, beacon_mac, sample_count, p50_delay_ms, p95_delay_ms, max_delay_ms, breach_count, threshold_ms
        FROM metric_delivery_delay
        WHERE run_id = ?
        ORDER BY p95_delay_ms DESC NULLS LAST
        LIMIT 20
        """,
        [run_id],
    ).fetchall()

    evidence = conn.execute(
        """
        SELECT
          tenant_id,
          event_type,
          beacon_mac,
          event_ts_ms,
          record_ts_ms,
          record_ts_ms - event_ts_ms AS delay_ms,
          record_uid
        FROM events_flat
        WHERE run_id = ? AND event_ts_ms IS NOT NULL AND record_ts_ms IS NOT NULL
        ORDER BY delay_ms DESC
        LIMIT 30
        """,
        [run_id],
    ).fetchall()

    return {
        "run": {
            "run_id": run_row[0],
            "app_id": run_row[1],
            "source_filename": run_row[2],
            "status": run_row[3],
            "total_lines": run_row[4],
            "valid_lines": run_row[5],
            "invalid_lines": run_row[6],
            "duplicate_lines": run_row[7],
        },
        "summary": {
            "flat_events": int(summary_row[0]),
            "distinct_beacons": int(summary_row[1]),
            "distinct_tenants": int(summary_row[2]),
        },
        "top_delivery_anomalies": [
            {
                "tenant_id": row[0],
                "event_type": row[1],
                "beacon_mac": row[2],
                "sample_count": int(row[3]),
                "p50_delay_ms": float(row[4]) if row[4] is not None else None,
                "p95_delay_ms": float(row[5]) if row[5] is not None else None,
                "max_delay_ms": int(row[6]) if row[6] is not None else None,
                "breach_count": int(row[7]),
                "threshold_ms": int(row[8]),
            }
            for row in top_delivery
        ],
        "sample_evidence_rows": [
            {
                "tenant_id": row[0],
                "event_type": row[1],
                "beacon_mac": row[2],
                "event_ts_ms": int(row[3]),
                "record_ts_ms": int(row[4]),
                "delay_ms": int(row[5]),
                "record_uid": row[6],
            }
            for row in evidence
        ],
    }


def _write_markdown(path: Path, content: str) -> None:
    path.write_text(content.strip() + "\n", encoding="utf-8")


def _try_render_pdf(markdown_text: str, target_path: Path) -> tuple[bool, str | None]:
    try:
        from reportlab.lib.pagesizes import letter  # type: ignore
        from reportlab.pdfgen import canvas  # type: ignore
    except Exception:
        return False, "PDF backend not installed (install reportlab to enable PDF export)."

    target_path.parent.mkdir(parents=True, exist_ok=True)
    doc = canvas.Canvas(str(target_path), pagesize=letter)
    text_obj = doc.beginText(40, 750)
    text_obj.setLeading(14)

    for line in markdown_text.splitlines():
        text_obj.textLine(line[:120])
        if text_obj.getY() < 60:
            doc.drawText(text_obj)
            doc.showPage()
            text_obj = doc.beginText(40, 750)
            text_obj.setLeading(14)

    doc.drawText(text_obj)
    doc.save()
    return True, None


def generate_report_artifacts(
    conn,
    settings: Settings,
    run_id: str,
    extra_instructions: str | None,
    create_pdf: bool,
) -> dict[str, Any]:
    settings.validate_reporting_credentials()
    payload = _build_report_payload(conn, run_id)
    base_prompt = _load_base_prompt(settings.base_prompt_path)
    system_prompt, user_prompt = _compose_prompts(base_prompt, extra_instructions)

    llm_input_payload = {
        "generated_at": _utc_now_iso(),
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "data": payload,
    }

    report_text = generate_report_text(
        settings=settings,
        system_prompt=system_prompt,
        user_prompt=(
            user_prompt
            + "\n\nData payload JSON:\n"
            + json.dumps(payload, indent=2, sort_keys=True)
            + "\n\nReturn markdown only."
        ),
    )

    output_dir = run_output_dir(settings.report_output_dir, run_id)
    prompt_snapshot_path = output_dir / "prompt_snapshot.txt"
    llm_input_path = output_dir / "llm_input_payload.json"
    report_md_path = output_dir / "report.md"

    prompt_snapshot_path.write_text(system_prompt + "\n\n" + user_prompt + "\n", encoding="utf-8")
    write_json(llm_input_path, llm_input_payload)
    _write_markdown(report_md_path, report_text)

    report_meta: dict[str, Any] = {
        "run_id": run_id,
        "generated_at": _utc_now_iso(),
        "report_markdown": str(report_md_path),
        "report_pdf": None,
        "pdf_status": "not_requested",
    }

    if create_pdf:
        pdf_path = output_dir / "report.pdf"
        ok, error = _try_render_pdf(report_text, pdf_path)
        if ok:
            report_meta["report_pdf"] = str(pdf_path)
            report_meta["pdf_status"] = "generated"
        else:
            report_meta["pdf_status"] = "skipped"
            report_meta["pdf_error"] = error

    write_json(output_dir / "report_metadata.json", report_meta)
    return report_meta
