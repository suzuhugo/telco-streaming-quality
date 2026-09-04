import apache_beam as beam
from apache_beam.testing.test_pipeline import (
    TestPipeline as BeamTestPipeline,
)
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from app.contracts import parse_utc
from app.transforms import (
    AddWindowMetadataDoFn,
    AssignEventTimestampDoFn,
    fixed_window_policy,
)


def make_event(event_id: str, event_time: str):
    return {
        "schema_version": 1,
        "event_id": event_id,
        "key": "NODE-01",
        "event_time": event_time,
        "emitted_at": "2026-09-03T18:40:00Z",
        "payload": {
            "node_id": "NODE-01",
            "latency_ms": 30.0,
            "packet_loss_pct": 0.5,
            "throughput_mbps": 90.0,
        },
    }


def test_event_time_assigns_correct_fixed_window():
    event = make_event(
        "E-001",
        "2026-09-03T18:34:05Z",
    )

    with BeamTestPipeline() as pipeline:
        result = (
            pipeline
            | "CreateEvent"
            >> beam.Create([event])
            | "AssignTimestamp"
            >> beam.ParDo(AssignEventTimestampDoFn())
            | "Window"
            >> fixed_window_policy(
                window_seconds=60,
                allowed_lateness_seconds=30,
            )
            | "AddWindowMetadata"
            >> beam.ParDo(AddWindowMetadataDoFn())
        )

        expected = [
            {
                **event,
                "window_start": (
                    "2026-09-03T18:34:00+00:00"
                ),
                "window_end": (
                    "2026-09-03T18:35:00+00:00"
                ),
            }
        ]

        assert_that(result, equal_to(expected))


def test_two_event_times_share_same_window():
    event_1 = make_event(
        "E-001",
        "2026-09-03T18:34:05Z",
    )

    event_2 = make_event(
        "E-002",
        "2026-09-03T18:34:55Z",
    )

    with BeamTestPipeline() as pipeline:
        result = (
            pipeline
            | "CreateTwoEvents"
            >> beam.Create([event_1, event_2])
            | "AssignTwoTimestamps"
            >> beam.ParDo(AssignEventTimestampDoFn())
            | "WindowTwoEvents"
            >> fixed_window_policy()
            | "MetadataTwoEvents"
            >> beam.ParDo(AddWindowMetadataDoFn())
        )

        window_pairs = result | "ExtractWindows" >> beam.Map(
            lambda event: (
                event["event_id"],
                event["window_start"],
                event["window_end"],
            )
        )

        assert_that(
            window_pairs,
            equal_to(
                [
                    (
                        "E-001",
                        "2026-09-03T18:34:00+00:00",
                        "2026-09-03T18:35:00+00:00",
                    ),
                    (
                        "E-002",
                        "2026-09-03T18:34:00+00:00",
                        "2026-09-03T18:35:00+00:00",
                    ),
                ]
            ),
        )


def test_window_boundary():
    before_boundary = make_event(
        "E-BEFORE",
        "2026-09-03T18:34:59Z",
    )

    at_boundary = make_event(
        "E-BOUNDARY",
        "2026-09-03T18:35:00Z",
    )

    with BeamTestPipeline() as pipeline:
        result = (
            pipeline
            | "CreateBoundaryEvents"
            >> beam.Create(
                [before_boundary, at_boundary]
            )
            | "AssignBoundaryTimestamps"
            >> beam.ParDo(AssignEventTimestampDoFn())
            | "BoundaryWindows"
            >> fixed_window_policy()
            | "BoundaryMetadata"
            >> beam.ParDo(AddWindowMetadataDoFn())
        )

        values = result | "ExtractBoundary" >> beam.Map(
            lambda event: (
                event["event_id"],
                event["window_start"],
            )
        )

        assert_that(
            values,
            equal_to(
                [
                    (
                        "E-BEFORE",
                        "2026-09-03T18:34:00+00:00",
                    ),
                    (
                        "E-BOUNDARY",
                        "2026-09-03T18:35:00+00:00",
                    ),
                ]
            ),
        )


def test_out_of_order_events_keep_event_time_windows():
    event_1 = make_event(
        "E-001",
        "2026-09-03T18:34:10Z",
    )

    event_2 = make_event(
        "E-002",
        "2026-09-03T18:34:40Z",
    )

    event_3 = make_event(
        "E-003",
        "2026-09-03T18:34:25Z",
    )

    ts_1 = parse_utc(
        event_1["event_time"]
    ).timestamp()

    ts_2 = parse_utc(
        event_2["event_time"]
    ).timestamp()

    ts_3 = parse_utc(
        event_3["event_time"]
    ).timestamp()

    stream = (
        BeamTestStream()
        .advance_watermark_to(ts_1 - 1)
        .add_elements(
            [
                TimestampedValue(event_1, ts_1),
                TimestampedValue(event_2, ts_2),
            ]
        )
        .add_elements(
            [
                TimestampedValue(event_3, ts_3),
            ]
        )
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline() as pipeline:
        result = (
            pipeline
            | "InputOutOfOrder"
            >> stream
            | "WindowOutOfOrder"
            >> fixed_window_policy()
            | "MetadataOutOfOrder"
            >> beam.ParDo(AddWindowMetadataDoFn())
        )

        windows = result | "ExtractOutOfOrder" >> beam.Map(
            lambda event: (
                event["event_id"],
                event["window_start"],
            )
        )

        assert_that(
            windows,
            equal_to(
                [
                    (
                        "E-001",
                        "2026-09-03T18:34:00+00:00",
                    ),
                    (
                        "E-002",
                        "2026-09-03T18:34:00+00:00",
                    ),
                    (
                        "E-003",
                        "2026-09-03T18:34:00+00:00",
                    ),
                ]
            ),
        )