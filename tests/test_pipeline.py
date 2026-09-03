import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

from app.contracts import serialize_event
from app.transforms import INVALID_TAG, ParseAndValidateDoFn


def make_valid_event():
    return {
        "schema_version": 1,
        "event_id": "NODE-01-20260903T170005-00001",
        "key": "NODE-01",
        "event_time": "2026-09-03T17:00:05Z",
        "emitted_at": "2026-09-03T17:00:06Z",
        "payload": {
            "node_id": "NODE-01",
            "latency_ms": 32.5,
            "packet_loss_pct": 0.7,
            "throughput_mbps": 92.4,
        },
    }


def test_valid_kafka_message_goes_to_main_output():
    event = make_valid_event()

    with BeamTestPipeline() as pipeline:
        outputs = (
            pipeline
            | "CreateValid"
            >> beam.Create(
                [
                    (
                        b"NODE-01",
                        serialize_event(event),
                    )
                ]
            )
            | "ValidateValid"
            >> beam.ParDo(
                ParseAndValidateDoFn()
            ).with_outputs(
                INVALID_TAG,
                main="valid",
            )
        )

        assert_that(
            outputs.valid,
            equal_to([event]),
            label="CheckValidOutput",
        )


def test_invalid_json_goes_to_invalid_output():
    with BeamTestPipeline() as pipeline:
        outputs = (
            pipeline
            | "CreateInvalidJson"
            >> beam.Create(
                [
                    (
                        b"NODE-01",
                        b'{"bad":',
                    )
                ]
            )
            | "ValidateInvalidJson"
            >> beam.ParDo(
                ParseAndValidateDoFn()
            ).with_outputs(
                INVALID_TAG,
                main="valid",
            )
        )

        errors = outputs.invalid | "ExtractJsonError" >> beam.Map(
            lambda row: row["error"]
        )

        assert_that(
            errors,
            equal_to(["JSON inválido"]),
            label="CheckInvalidJson",
        )


def test_kafka_key_mismatch_goes_to_invalid_output():
    event = make_valid_event()

    with BeamTestPipeline() as pipeline:
        outputs = (
            pipeline
            | "CreateWrongKey"
            >> beam.Create(
                [
                    (
                        b"NODE-99",
                        serialize_event(event),
                    )
                ]
            )
            | "ValidateWrongKey"
            >> beam.ParDo(
                ParseAndValidateDoFn()
            ).with_outputs(
                INVALID_TAG,
                main="valid",
            )
        )

        errors = outputs.invalid | "ExtractKeyError" >> beam.Map(
            lambda row: row["error"]
        )

        assert_that(
            errors,
            equal_to(
                [
                    "Kafka key 'NODE-99' no coincide con event.key 'NODE-01'"
                ]
            ),
            label="CheckWrongKey",
        )


def test_null_kafka_value_goes_to_invalid_output():
    with BeamTestPipeline() as pipeline:
        outputs = (
            pipeline
            | "CreateNullValue"
            >> beam.Create(
                [
                    (
                        b"NODE-01",
                        None,
                    )
                ]
            )
            | "ValidateNullValue"
            >> beam.ParDo(
                ParseAndValidateDoFn()
            ).with_outputs(
                INVALID_TAG,
                main="valid",
            )
        )

        errors = outputs.invalid | "ExtractNullError" >> beam.Map(
            lambda row: row["error"]
        )

        assert_that(
            errors,
            equal_to(["Kafka value ausente"]),
            label="CheckNullValue",
        )