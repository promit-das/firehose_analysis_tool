from __future__ import annotations

from app.extractors import deep_get, extract_flat_record, parse_raw_record, to_epoch_ms


def test_deep_get_with_dict_and_list() -> None:
    payload = {"a": {"b": [{"c": 7}]}}
    assert deep_get(payload, "a.b.0.c") == 7
    assert deep_get(payload, "a.b.1.c") is None


def test_to_epoch_ms_numeric_and_iso() -> None:
    assert to_epoch_ms(1700000000) == 1700000000 * 1000
    assert to_epoch_ms(1700000000000) == 1700000000000
    assert to_epoch_ms("2024-01-01T00:00:00Z") == 1704067200000


def test_parse_raw_record_fallback_uid_is_stable() -> None:
    record = {"tenant_id": "t1", "event_type": "button_press", "record_ts_ms": 1700000000123}
    parsed_a = parse_raw_record(record, '{"tenant_id":"t1"}')
    parsed_b = parse_raw_record(record, '{"tenant_id":"t1"}')
    assert parsed_a["record_uid"] == parsed_b["record_uid"]


def test_extract_flat_record_uses_event_specific_rules() -> None:
    payload = {
        "beacon": {"mac": "AA:BB"},
        "button": {"ts_ms": 1700000001111},
        "location": {"id": "L1"},
    }
    rules = {
        "default": {
            "event_ts_paths": ["event_ts_ms"],
            "beacon_mac_paths": ["beacon.mac"],
            "location_id_paths": ["location.id"],
        },
        "event_types": {
            "button_press": {
                "event_ts_paths": ["button.ts_ms"],
            }
        },
    }

    flat = extract_flat_record(payload, "button_press", 9999, rules)
    assert flat["event_ts_ms"] == 1700000001111
    assert flat["beacon_mac"] == "AA:BB"
    assert flat["location_id"] == "L1"
