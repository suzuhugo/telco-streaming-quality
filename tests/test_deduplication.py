import copy

import apache_beam as beam
from apache_beam.testing.test_pipeline import (
    TestPipeline as BeamTestPipeline,
)
from apache_beam.testing.util import assert_that, equal_to

from app.transforms import (
    AssignEventTimestampDoFn,
    deduplicate_by_event_id,
    fixed_window_policy,
)


def make_event(
    event_id: str,
    *,
    node_id: str = "NODE-01",
):
    return {
        "schema_version": 1,
        "event_id": event_id,
        "key": node_id,
        "event_time": "2026-09-05T12:30:10Z",
        "emitted_at": "2026-09-05T12:30:11Z",
        "payload": {
            "node_id": node_id,
            "latency_ms": 30.0,
            "packet_loss_pct": 0.5,
            "throughput_mbps": 90.0,
        },
    }

def test_duplicate_event_id_is_emitted_once():
    event = make_event("E-001")
    duplicate = copy.deepcopy(event)

    with BeamTestPipeline() as pipeline:
        source = (
            pipeline
            | "CreateDuplicateEvents"
            >> beam.Create(
                [
                    event,
                    duplicate,
                ]
            )
        )

        result = deduplicate_by_event_id(source)

        ids = result | "ExtractUniqueIds" >> beam.Map(
            lambda item: item["event_id"]
        )

        assert_that(
            ids,
            equal_to(["E-001"]),
        )


def test_different_event_ids_are_preserved():
    event_1 = make_event("E-001")
    event_2 = make_event("E-002")
    event_3 = make_event("E-003")

    with BeamTestPipeline() as pipeline:
        source = (
            pipeline
            | "CreateDistinctEvents"
            >> beam.Create(
                [
                    event_1,
                    event_2,
                    event_3,
                ]
            )
        )

        result = deduplicate_by_event_id(source)

        ids = result | "ExtractDistinctIds" >> beam.Map(
            lambda item: item["event_id"]
        )

        assert_that(
            ids,
            equal_to(
                [
                    "E-001",
                    "E-002",
                    "E-003",
                ]
            ),
        )


def test_multiple_duplicates_still_emit_one_event():
    event = make_event("E-001")

    with BeamTestPipeline() as pipeline:
        source = (
            pipeline
            | "CreateManyDuplicates"
            >> beam.Create(
                [
                    copy.deepcopy(event),
                    copy.deepcopy(event),
                    copy.deepcopy(event),
                    copy.deepcopy(event),
                ]
            )
        )

        result = deduplicate_by_event_id(source)

        ids = result | "ExtractSingleId" >> beam.Map(
            lambda item: item["event_id"]
        )

        assert_that(
            ids,
            equal_to(["E-001"]),
        )


def test_duplicate_is_removed_inside_fixed_window():
    event = make_event("E-WINDOW")
    duplicate = copy.deepcopy(event)

    with BeamTestPipeline() as pipeline:
        windowed = (
            pipeline
            | "CreateWindowDuplicate"
            >> beam.Create(
                [
                    event,
                    duplicate,
                ]
            )
            | "AssignWindowTimestamp"
            >> beam.ParDo(AssignEventTimestampDoFn())
            | "ApplyDedupWindow"
            >> fixed_window_policy(
                window_seconds=60,
                allowed_lateness_seconds=30,
            )
        )

        result = deduplicate_by_event_id(windowed)

        ids = result | "ExtractWindowUniqueIds" >> beam.Map(
            lambda item: item["event_id"]
        )

        assert_that(
            ids,
            equal_to(["E-WINDOW"]),
        )
