import json

from app.output import (
    make_aggregate_key,
    materialize_upserts,
    to_kafka_record,
)


def make_result(
    *,
    node_id="NODE-01",
    window_start="2026-09-06T10:00:00+00:00",
    window_end="2026-09-06T10:01:00+00:00",
    pane_index=0,
    pane_timing="ON_TIME",
    sample_count=2,
    avg_latency=30.0,
):
    return {
        "schema_version": 1,
        "metric_type": "network_quality",
        "node_id": node_id,
        "window_start": window_start,
        "window_end": window_end,
        "pane_index": pane_index,
        "pane_timing": pane_timing,
        "sample_count": sample_count,
        "avg_latency_ms": avg_latency,
        "avg_packet_loss_pct": 1.0,
        "avg_throughput_mbps": 90.0,
    }

def test_on_time_and_late_have_same_key():
    on_time = make_result(
        pane_index=0,
        pane_timing="ON_TIME",
        sample_count=2,
    )

    late = make_result(
        pane_index=1,
        pane_timing="LATE",
        sample_count=3,
    )

    assert (
        make_aggregate_key(on_time)
        == make_aggregate_key(late)
    )

def test_different_nodes_have_different_keys():
    node_1 = make_result(
        node_id="NODE-01"
    )

    node_2 = make_result(
        node_id="NODE-02"
    )

    assert (
        make_aggregate_key(node_1)
        != make_aggregate_key(node_2)
    )

def test_different_windows_have_different_keys():
    window_1 = make_result(
        window_start=(
            "2026-09-06T10:00:00+00:00"
        ),
        window_end=(
            "2026-09-06T10:01:00+00:00"
        ),
    )

    window_2 = make_result(
        window_start=(
            "2026-09-06T10:01:00+00:00"
        ),
        window_end=(
            "2026-09-06T10:02:00+00:00"
        ),
    )

    assert (
        make_aggregate_key(window_1)
        != make_aggregate_key(window_2)
    )

def test_late_pane_replaces_on_time():
    on_time = make_result(
        pane_index=0,
        pane_timing="ON_TIME",
        sample_count=2,
        avg_latency=30.0,
    )

    late = make_result(
        pane_index=1,
        pane_timing="LATE",
        sample_count=3,
        avg_latency=40.0,
    )

    store = materialize_upserts(
        [
            on_time,
            late,
        ]
    )

    assert len(store) == 1

    key = make_aggregate_key(on_time)

    assert store[key]["pane_timing"] == "LATE"
    assert store[key]["sample_count"] == 3
    assert store[key]["avg_latency_ms"] == 40.0

def test_retry_does_not_duplicate_entity():
    result = make_result()

    store = materialize_upserts(
        [
            result,
            result,
        ]
    )

    assert len(store) == 1

def test_kafka_record_uses_stable_key_and_json_value():
    result = make_result()

    key, value = to_kafka_record(result)

    assert key.decode("utf-8") == (
        "network_quality|"
        "NODE-01|"
        "2026-09-06T10:00:00+00:00"
    )

    recovered = json.loads(
        value.decode("utf-8")
    )

    assert recovered == result
