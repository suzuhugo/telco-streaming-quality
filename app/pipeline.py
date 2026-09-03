import argparse
import logging
from typing import Any

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    PortableOptions,
    StandardOptions,
)

from app.transforms import INVALID_TAG, ParseAndValidateDoFn

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_INPUT_TOPIC = "telco.telemetry.v1"
DEFAULT_GROUP_ID = "telco-quality-pipeline-v1"

LOGGER = logging.getLogger(__name__)


def log_valid_event(event: dict[str, Any]) -> dict[str, Any]:
    LOGGER.info(
        "VALID event_id=%s key=%s event_time=%s",
        event["event_id"],
        event["key"],
        event["event_time"],
    )

    return event


def log_invalid_event(record: dict[str, Any]) -> dict[str, Any]:
    LOGGER.warning(
        "INVALID kafka_key=%s error=%s raw=%s",
        record["kafka_key"],
        record["error"],
        record["raw_value"],
    )

    return record


def build_pipeline(
    pipeline: beam.Pipeline,
    *,
    bootstrap_servers: str,
    input_topic: str,
    group_id: str,
    offset_reset: str,
):
    """Construir la fase de lectura y validación."""

    kafka_messages = (
        pipeline
        | "ReadFromKafka"
        >> ReadFromKafka(
            consumer_config={
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": offset_reset,
                "enable.auto.commit": "false",
            },
            topics=[input_topic],
            with_metadata=False,
        )
    )

    outputs = (
        kafka_messages
        | "ParseAndValidate"
        >> beam.ParDo(
            ParseAndValidateDoFn()
        ).with_outputs(
            INVALID_TAG,
            main="valid",
        )
    )

    _ = (
        outputs.valid
        | "LogValidEvents"
        >> beam.Map(log_valid_event)
    )

    _ = (
        outputs.invalid
        | "LogInvalidEvents"
        >> beam.Map(log_invalid_event)
    )

    return outputs.valid, outputs.invalid


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Pipeline Beam para telemetría "
            "de calidad de red"
        )
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
    )

    parser.add_argument(
        "--input-topic",
        default=DEFAULT_INPUT_TOPIC,
    )

    parser.add_argument(
        "--group-id",
        default=DEFAULT_GROUP_ID,
    )

    parser.add_argument(
        "--offset-reset",
        choices=("latest", "earliest"),
        default="latest",
    )

    return parser.parse_known_args()


def main() -> None:
    args, beam_args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s "
            "%(levelname)s "
            "%(name)s "
            "%(message)s"
        ),
    )

    pipeline_options = PipelineOptions(beam_args)

    standard_options = pipeline_options.view_as(
        StandardOptions
    )

    portable_options = pipeline_options.view_as(
        PortableOptions
    )

    if not standard_options.runner:
        standard_options.runner = "PrismRunner"

    standard_options.streaming = True

    if not portable_options.environment_type:
        portable_options.environment_type = "LOOPBACK"

    pipeline = beam.Pipeline(
        options=pipeline_options
    )

    build_pipeline(
        pipeline,
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.input_topic,
        group_id=args.group_id,
        offset_reset=args.offset_reset,
    )

    LOGGER.info(
        "Starting pipeline topic=%s "
        "bootstrap=%s group=%s",
        args.input_topic,
        args.bootstrap_servers,
        args.group_id,
    )

    result = pipeline.run()

    try:
        result.wait_until_finish()
    except KeyboardInterrupt:
        LOGGER.info("Pipeline detenido por el usuario.")
       

if __name__ == "__main__":
    main()