import apache_beam as beam
from apache_beam.transforms import trigger

from app.transforms import aggregation_window_policy

from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
)
from apache_beam.testing.test_pipeline import (
    TestPipeline as BeamTestPipeline,
)
from apache_beam.testing.test_stream import (
    TestStream as BeamTestStream,
)
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from app.contracts import parse_utc
from app.transforms import (
    FormatQualityAggregateDoFn,
    aggregate_quality_by_node,
)

def test_aggregation_trigger_policy():
    policy = aggregation_window_policy(
        window_seconds=60,
        allowed_lateness_seconds=30,
    )

    assert isinstance(policy, beam.WindowInto)

    assert (
        policy.windowing.windowfn.size.micros
        == 60_000_000
    )

    assert (
        policy.windowing.allowed_lateness.micros
        == 30_000_000
    )

    assert (
        policy.windowing.accumulation_mode
        == trigger.AccumulationMode.ACCUMULATING
    )

    assert isinstance(
        policy.windowing.triggerfn,
        trigger.AfterWatermark,
    )


def make_event(
    event_id,
    *,
    event_time,
    latency,
):
    return {
        "schema_version": 1,
        "event_id": event_id,
        "key": "NODE-01",
        "event_time": event_time,
        "emitted_at": "2026-09-05T16:02:00Z",
        "payload": {
            "node_id": "NODE-01",
            "latency_ms": latency,
            "packet_loss_pct": 1.0,
            "throughput_mbps": 90.0,
        },
    }


def streaming_options():
    options = PipelineOptions()

    options.view_as(
        StandardOptions
    ).streaming = True

    return options


def test_late_event_updates_accumulating_pane():
    event_1 = make_event(
        "E-001",
        event_time="2026-09-05T16:00:10Z",
        latency=20.0,
    )

    event_2 = make_event(
        "E-002",
        event_time="2026-09-05T16:00:20Z",
        latency=40.0,
    )

    late_event = make_event(
        "E-003",
        event_time="2026-09-05T16:00:30Z",
        latency=60.0,
    )

    ts_1 = parse_utc(
        event_1["event_time"]
    ).timestamp()

    ts_2 = parse_utc(
        event_2["event_time"]
    ).timestamp()

    ts_late = parse_utc(
        late_event["event_time"]
    ).timestamp()

    window_end = parse_utc(
        "2026-09-05T16:01:00Z"
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
        .advance_watermark_to(window_end)
        .add_elements(
            [
                TimestampedValue(
                    late_event,
                    ts_late,
                )
            ]
        )
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(
        options=streaming_options()
    ) as pipeline:
        windowed = (
            pipeline
            | "TriggerInput"
            >> stream
            | "TriggerWindow"
            >> aggregation_window_policy(
                window_seconds=60,
                allowed_lateness_seconds=30,
            )
        )

        aggregated = aggregate_quality_by_node(
            windowed
        )

        formatted = (
            aggregated
            | "FormatTriggeredAggregate"
            >> beam.ParDo(
                FormatQualityAggregateDoFn()
            )
        )

        projection = (
            formatted
            | "ProjectTriggeredResults"
            >> beam.Map(
                lambda item: (
                    item["pane_timing"],
                    item["sample_count"],
                    item["avg_latency_ms"],
                )
            )
        )

        assert_that(
            projection,
            equal_to(
                [
                    (
                        "ON_TIME",
                        2,
                        30.0,
                    ),
                    (
                        "LATE",
                        3,
                        40.0,
                    ),
                ]
            ),
        )


def test_event_beyond_allowed_lateness_is_not_revised():
    event_1 = make_event(
        "E-001",
        event_time="2026-09-05T16:00:10Z",
        latency=20.0,
    )

    too_late_event = make_event(
        "E-TOO-LATE",
        event_time="2026-09-05T16:00:20Z",
        latency=100.0,
    )

    ts_1 = parse_utc(
        event_1["event_time"]
    ).timestamp()

    ts_too_late = parse_utc(
        too_late_event["event_time"]
    ).timestamp()

    window_end = parse_utc(
        "2026-09-05T16:01:00Z"
    ).timestamp()

    after_gc = parse_utc(
        "2026-09-05T16:01:31Z"
    ).timestamp()

    stream = (
        BeamTestStream()
        .advance_watermark_to(ts_1 - 1)
        .add_elements(
            [
                TimestampedValue(event_1, ts_1),
            ]
        )
        .advance_watermark_to(window_end)
        .advance_watermark_to(after_gc)
        .add_elements(
            [
                TimestampedValue(
                    too_late_event,
                    ts_too_late,
                )
            ]
        )
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(
        options=streaming_options()
    ) as pipeline:
        windowed = (
            pipeline
            | "TooLateInput"
            >> stream
            | "TooLateWindow"
            >> aggregation_window_policy(
                window_seconds=60,
                allowed_lateness_seconds=30,
            )
        )

        aggregated = aggregate_quality_by_node(
            windowed
        )

        formatted = (
            aggregated
            | "FormatTooLateAggregate"
            >> beam.ParDo(
                FormatQualityAggregateDoFn()
            )
        )

        projection = (
            formatted
            | "ProjectTooLate"
            >> beam.Map(
                lambda item: (
                    item["pane_timing"],
                    item["sample_count"],
                    item["avg_latency_ms"],
                )
            )
        )

        assert_that(
            projection,
            equal_to(
                [
                    (
                        "ON_TIME",
                        1,
                        20.0,
                    )
                ]
            ),
        )