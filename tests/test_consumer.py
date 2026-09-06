import json

import pytest

from app.consumer import (
    apply_upsert,
    decode_kafka_key,
    deserialize_quality_aggregate,
)
from app.output import make_aggregate_key


def make_result(
    *,
    node_id="NODE-01",
    window_start="2026-09-06T11:00:00+00:00",
    window_end="2026-09-06T11:01:00+00:00",
    pane_index=0,
    pane_timing="ON_TIME",
    sample_count=2,
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
        "avg_latency_ms": 30.0,
        "avg_packet_loss_pct": 1.0,
        "avg_throughput_mbps": 90.0,
    }

def test_first_revision_inserts_entity():
    current_view = {}

    result = make_result()
    key = make_aggregate_key(result)

    operation = apply_upsert(
        current_view,
        kafka_key=key,
        aggregate=result,
    )

    assert operation == "INSERT"
    assert len(current_view) == 1
    assert current_view[key] == result

def test_late_revision_updates_same_entity():
    current_view = {}

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

    key = make_aggregate_key(on_time)

    first_operation = apply_upsert(
        current_view,
        kafka_key=key,
        aggregate=on_time,
    )

    second_operation = apply_upsert(
        current_view,
        kafka_key=key,
        aggregate=late,
    )

    assert first_operation == "INSERT"
    assert second_operation == "UPDATE"

    assert len(current_view) == 1

    assert (
        current_view[key]["pane_timing"]
        == "LATE"
    )

    assert (
        current_view[key]["sample_count"]
        == 3
    )

def test_retry_keeps_single_entity():
    current_view = {}

    result = make_result()
    key = make_aggregate_key(result)

    apply_upsert(
        current_view,
        kafka_key=key,
        aggregate=result,
    )

    operation = apply_upsert(
        current_view,
        kafka_key=key,
        aggregate=result,
    )

    assert operation == "UPDATE"
    assert len(current_view) == 1

def test_different_nodes_create_different_entities():
    current_view = {}

    node_1 = make_result(
        node_id="NODE-01"
    )

    node_2 = make_result(
        node_id="NODE-02"
    )

    key_1 = make_aggregate_key(node_1)
    key_2 = make_aggregate_key(node_2)

    apply_upsert(
        current_view,
        kafka_key=key_1,
        aggregate=node_1,
    )

    apply_upsert(
        current_view,
        kafka_key=key_2,
        aggregate=node_2,
    )

    assert len(current_view) == 2

def test_wrong_kafka_key_is_rejected():
    current_view = {}

    result = make_result()

    with pytest.raises(
        ValueError,
        match="no coincide",
    ):
        apply_upsert(
            current_view,
            kafka_key="wrong-key",
            aggregate=result,
        )

    assert current_view == {}

def test_deserialize_quality_aggregate():
    result = make_result()

    raw = json.dumps(result).encode("utf-8")

    recovered = deserialize_quality_aggregate(
        raw
    )

    assert recovered == result

def test_decode_kafka_key():
    result = make_result()
    key = make_aggregate_key(result)

    assert (
        decode_kafka_key(
            key.encode("utf-8")
        )
        == key
    )