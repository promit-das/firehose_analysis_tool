from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any


DEFAULT_RECORD_UID_PATHS = [
    "record_uid",
    "recordUid",
    "id",
    "message_id",
    "metadata.record_uid",
]

DEFAULT_APP_ID_PATHS = [
    "app_id",
    "appId",
    "applicationId",
    "application.id",
    "partnerTenantId",
    "spacesTenantId",
]

DEFAULT_TENANT_PATHS = [
    "spacesTenantId",
    "spacesTenantName",
    "partnerTenantId",
    "tenant_id",
    "tenantId",
    "tenant.id",
    "metadata.tenant_id",
]

DEFAULT_EVENT_TYPE_PATHS = [
    "event_type",
    "eventType",
    "type",
    "event.type",
    "metadata.event_type",
]

DEFAULT_RECORD_TS_PATHS = [
    "recordTimestamp",
    "record_ts",
    "record_timestamp",
    "record_ts_ms",
    "recordTsMs",
    "record.timestamp_ms",
    "record.timestamp",
    "timestamp_ms",
    "timestamp",
    "metadata.record_ts_ms",
]


def load_extractor_rules(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def canonical_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def deep_get(payload: Any, dotted_path: str) -> Any:
    current = payload
    for part in dotted_path.split("."):
        if isinstance(current, dict):
            if part not in current:
                return None
            current = current[part]
            continue

        if isinstance(current, list):
            if not part.isdigit():
                return None
            index = int(part)
            if index < 0 or index >= len(current):
                return None
            current = current[index]
            continue

        return None
    return current


def first_value(payload: Any, paths: list[str]) -> Any:
    for path in paths:
        value = deep_get(payload, path)
        if value is not None:
            return value
    return None


def to_epoch_ms(value: Any) -> int | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, int):
        if value > 10_000_000_000:
            return value
        return value * 1000

    if isinstance(value, float):
        return int(value) if value > 10_000_000_000 else int(value * 1000)

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.isdigit():
            return to_epoch_ms(int(raw))
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)

    return None


def derive_record_uid(record: dict[str, Any], canonical_line: str) -> str:
    raw_uid = first_value(record, DEFAULT_RECORD_UID_PATHS)
    if raw_uid is not None:
        return str(raw_uid)
    return hashlib.sha256(canonical_line.encode("utf-8")).hexdigest()


def parse_raw_record(
    record: dict[str, Any],
    original_line: str,
    default_app_id: str = "unknown_app",
) -> dict[str, Any]:
    canonical_line = canonical_json(record)
    record_uid = derive_record_uid(record, canonical_line)
    app_id = first_value(record, DEFAULT_APP_ID_PATHS)
    tenant_id = first_value(record, DEFAULT_TENANT_PATHS)
    event_type = first_value(record, DEFAULT_EVENT_TYPE_PATHS)
    record_ts_ms = to_epoch_ms(first_value(record, DEFAULT_RECORD_TS_PATHS))

    return {
        "record_uid": record_uid,
        "app_id": str(app_id) if app_id is not None else default_app_id,
        "tenant_id": str(tenant_id) if tenant_id is not None else None,
        "event_type": str(event_type) if event_type is not None else None,
        "record_ts_ms": record_ts_ms,
        "payload_json": canonical_line,
        "raw_line": original_line,
    }


def _event_rule_paths(rules: dict[str, Any], event_type: str | None, field: str) -> list[str]:
    defaults = rules.get("default", {})
    event_rules = rules.get("event_types", {}).get(event_type or "", {})
    paths = event_rules.get(field)
    if paths:
        return list(paths)
    return list(defaults.get(field, []))


def extract_flat_record(
    payload: dict[str, Any],
    event_type: str | None,
    record_ts_ms: int | None,
    rules: dict[str, Any],
) -> dict[str, Any]:
    event_ts_paths = _event_rule_paths(rules, event_type, "event_ts_paths")
    beacon_paths = _event_rule_paths(rules, event_type, "beacon_mac_paths")
    location_paths = _event_rule_paths(rules, event_type, "location_id_paths")

    event_ts_raw = first_value(payload, event_ts_paths)
    event_ts_ms = to_epoch_ms(event_ts_raw)
    if event_ts_ms is None:
        event_ts_ms = record_ts_ms

    beacon_raw = first_value(payload, beacon_paths)
    location_raw = first_value(payload, location_paths)

    return {
        "event_ts_ms": event_ts_ms,
        "beacon_mac": str(beacon_raw) if beacon_raw is not None else None,
        "location_id": str(location_raw) if location_raw is not None else None,
    }
