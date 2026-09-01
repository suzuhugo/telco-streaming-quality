from datetime import UTC

import pytest

from app.contracts import (
    deserialize_event,
    parse_utc,
    serialize_event,
    validate_event,
)


def test_parse_utc_valid_timestamp():
    result = parse_utc("2026-09-01T18:34:05Z")

    assert result.year == 2026
    assert result.month == 9
    assert result.day == 1
    assert result.hour == 18
    assert result.minute == 34
    assert result.second == 5
    assert result.tzinfo == UTC


def test_parse_utc_rejects_non_utc_timestamp():
    with pytest.raises(ValueError):
        parse_utc("2026-09-01T18:34:05")


def make_valid_event():
    return {
        "schema_version": 1,
        "event_id": "NODE-01-20260901T183405-001",
        "key": "NODE-01",
        "event_time": "2026-09-01T18:34:05Z",
        "emitted_at": "2026-09-01T18:34:06Z",
        "payload": {
            "node_id": "NODE-01",
            "latency_ms": 32.5,
            "packet_loss_pct": 0.7,
            "throughput_mbps": 92.4,
        },
    }


def test_validate_event_accepts_valid_event():
    event = make_valid_event()

    validate_event(event)


def test_validate_event_rejects_missing_event_id():
    event = make_valid_event()
    del event["event_id"]

    with pytest.raises(ValueError, match="event_id"):
        validate_event(event)


def test_validate_event_rejects_key_node_mismatch():
    event = make_valid_event()
    event["payload"]["node_id"] = "NODE-02"

    with pytest.raises(
        ValueError,
        match="key debe coincidir",
    ):
        validate_event(event)


def test_validate_event_rejects_negative_latency():
    event = make_valid_event()
    event["payload"]["latency_ms"] = -5

    with pytest.raises(ValueError, match="latency_ms"):
        validate_event(event)


def test_validate_event_rejects_invalid_packet_loss():
    event = make_valid_event()
    event["payload"]["packet_loss_pct"] = 120

    with pytest.raises(ValueError, match="packet_loss_pct"):
        validate_event(event)


def test_validate_event_rejects_negative_throughput():
    event = make_valid_event()
    event["payload"]["throughput_mbps"] = -1

    with pytest.raises(ValueError, match="throughput_mbps"):
        validate_event(event)


def test_serialize_deserialize_round_trip():
    original = make_valid_event()

    serialized = serialize_event(original)
    recovered = deserialize_event(serialized)

    assert isinstance(serialized, bytes)
    assert recovered == original


def test_deserialize_rejects_invalid_json():
    raw = b'{"event_id": '

    with pytest.raises(ValueError, match="JSON inválido"):
        deserialize_event(raw)