import copy

import apache_beam as beam
from apache_beam.testing.test_pipeline import (
    TestPipeline as BeamTestPipeline,
)
from apache_beam.testing.util import assert_that, equal_to

from app.transforms import (
    AssignEventTimestampDoFn,
    FormatQualityAggregateDoFn,
    aggregate_quality_by_node,
    deduplicate_by_event_id,
    fixed_window_policy,
)


def make_event(
    event_id,
    *,
    node_id="NODE-01",
    event_time="2026-09-05T16:00:10Z",
    latency=30.0,
    packet_loss=0.5,
    throughput=90.0,
):
    return {
        "schema_version": 1,
        "event_id": event_id,
        "key": node_id,
        "event_time": event_time,
        "emitted_at": "2026-09-05T16:00:11Z",
        "payload": {
            "node_id": node_id,
            "latency_ms": latency,
            "packet_loss_pct": packet_loss,
            "throughput_mbps": throughput,
        },
    }


def test_quality_aggregation_for_one_node():
    event_1 = make_event(
        "E-001",
        latency=20.0,
        packet_loss=0.5,
        throughput=80.0,
    )

    event_2 = make_event(
        "E-002",
        latency=40.0,
        packet_loss=1.5,
        throughput=100.0,
    )

    with BeamTestPipeline() as pipeline:
        source = (
            pipeline
            | "CreateOneNodeEvents"
            >> beam.Create([event_1, event_2])
        )

        result = aggregate_quality_by_node(source)

        assert_that(
            result,
            equal_to(
                [
                    (
                        "NODE-01",
                        {
                            "sample_count": 2,
                            "avg_latency_ms": 30.0,
                            "avg_packet_loss_pct": 1.0,
                            "avg_throughput_mbps": 90.0,
                        },
                    )
                ]
            ),
        )


def test_quality_aggregation_isolated_by_node():
    event_1 = make_event(
        "E-001",
        node_id="NODE-01",
        latency=20.0,
        packet_loss=0.5,
        throughput=80.0,
    )

    event_2 = make_event(
        "E-002",
        node_id="NODE-01",
        latency=40.0,
        packet_loss=1.5,
        throughput=100.0,
    )

    event_3 = make_event(
        "E-003",
        node_id="NODE-02",
        latency=50.0,
        packet_loss=2.0,
        throughput=70.0,
    )

    with BeamTestPipeline() as pipeline:
        source = (
            pipeline
            | "CreateMultipleNodes"
            >> beam.Create(
                [
                    event_1,
                    event_2,
                    event_3,
                ]
            )
        )

        result = aggregate_quality_by_node(source)

        assert_that(
            result,
            equal_to(
                [
                    (
                        "NODE-01",
                        {
                            "sample_count": 2,
                            "avg_latency_ms": 30.0,
                            "avg_packet_loss_pct": 1.0,
                            "avg_throughput_mbps": 90.0,
                        },
                    ),
                    (
                        "NODE-02",
                        {
                            "sample_count": 1,
                            "avg_latency_ms": 50.0,
                            "avg_packet_loss_pct": 2.0,
                            "avg_throughput_mbps": 70.0,
                        },
                    ),
                ]
            ),
        )


def test_duplicate_does_not_change_aggregate():
    event_1 = make_event(
        "E-001",
        latency=20.0,
        packet_loss=0.5,
        throughput=80.0,
    )

    duplicate = copy.deepcopy(event_1)

    event_2 = make_event(
        "E-002",
        event_time="2026-09-05T16:00:20Z",
        latency=40.0,
        packet_loss=1.5,
        throughput=100.0,
    )

    with BeamTestPipeline() as pipeline:
        windowed = (
            pipeline
            | "CreateDuplicateAggregate"
            >> beam.Create(
                [
                    event_1,
                    duplicate,
                    event_2,
                ]
            )
            | "AssignAggregateTimestamp"
            >> beam.ParDo(AssignEventTimestampDoFn())
            | "AggregateWindow"
            >> fixed_window_policy(
                window_seconds=60,
                allowed_lateness_seconds=30,
            )
        )

        unique = deduplicate_by_event_id(windowed)

        result = aggregate_quality_by_node(unique)

        assert_that(
            result,
            equal_to(
                [
                    (
                        "NODE-01",
                        {
                            "sample_count": 2,
                            "avg_latency_ms": 30.0,
                            "avg_packet_loss_pct": 1.0,
                            "avg_throughput_mbps": 90.0,
                        },
                    )
                ]
            ),
        )


def test_formatted_aggregate_contains_window_metadata():
    event_1 = make_event(
        "E-001",
        event_time="2026-09-05T16:00:10Z",
        latency=20.0,
        packet_loss=0.5,
        throughput=80.0,
    )

    event_2 = make_event(
        "E-002",
        event_time="2026-09-05T16:00:30Z",
        latency=40.0,
        packet_loss=1.5,
        throughput=100.0,
    )

    with BeamTestPipeline() as pipeline:
        windowed = (
            pipeline
            | "CreateFormattedAggregate"
            >> beam.Create([event_1, event_2])
            | "AssignFormattedTimestamp"
            >> beam.ParDo(AssignEventTimestampDoFn())
            | "FormattedWindow"
            >> fixed_window_policy()
        )

        aggregated = aggregate_quality_by_node(windowed)

        result = (
            aggregated
            | "FormatAggregate"
            >> beam.ParDo(FormatQualityAggregateDoFn())
        )


        projection = (
            result
            | "ProjectFormattedFields"
            >> beam.Map(
                lambda item: (
                    item["schema_version"],
                    item["metric_type"],
                    item["node_id"],
                    item["window_start"],
                    item["window_end"],
                    item["sample_count"],
                    item["avg_latency_ms"],
                    item["avg_packet_loss_pct"],
                    item["avg_throughput_mbps"],
                )
            )
        )

        assert_that(
            projection,
            equal_to(
                [
                    (
                        1,
                        "network_quality",
                        "NODE-01",
                        "2026-09-05T16:00:00+00:00",
                        "2026-09-05T16:01:00+00:00",
                        2,
                        30.0,
                        1.0,
                        90.0,
                    )
                ]
            ),
        )

        

def test_same_node_produces_separate_window_aggregates():
    event_1 = make_event(
        "E-W1",
        event_time="2026-09-05T16:00:10Z",
        latency=20.0,
    )

    event_2 = make_event(
        "E-W2",
        event_time="2026-09-05T16:01:10Z",
        latency=50.0,
    )

    with BeamTestPipeline() as pipeline:
        windowed = (
            pipeline
            | "CreateTwoWindows"
            >> beam.Create([event_1, event_2])
            | "AssignTwoWindowTimestamps"
            >> beam.ParDo(AssignEventTimestampDoFn())
            | "ApplyTwoWindows"
            >> fixed_window_policy()
        )

        aggregated = aggregate_quality_by_node(windowed)

        result = (
            aggregated
            | "FormatTwoWindowAggregates"
            >> beam.ParDo(FormatQualityAggregateDoFn())
        )

        projection = result | "ProjectWindowResults" >> beam.Map(
            lambda item: (
                item["node_id"],
                item["window_start"],
                item["sample_count"],
                item["avg_latency_ms"],
            )
        )

        assert_that(
            projection,
            equal_to(
                [
                    (
                        "NODE-01",
                        "2026-09-05T16:00:00+00:00",
                        1,
                        20.0,
                    ),
                    (
                        "NODE-01",
                        "2026-09-05T16:01:00+00:00",
                        1,
                        50.0,
                    ),
                ]
            ),
        )