import argparse
import logging
from typing import Any

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    PortableOptions,
    StandardOptions,
)

from app.output import to_kafka_record
from app.transforms import (
    INVALID_TAG,
    AssignEventTimestampDoFn,
    FormatQualityAggregateDoFn,
    ParseAndValidateDoFn,
    aggregate_quality_by_node,
    aggregation_window_policy,
    deduplicate_by_event_id,
    fixed_window_policy,
)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_INPUT_TOPIC = "telco.telemetry.v1"
DEFAULT_GROUP_ID = "telco-quality-pipeline-v1"
DEFAULT_OUTPUT_TOPIC = "telco.quality.v1"

LOGGER = logging.getLogger(__name__)


def log_valid_event(event: dict[str, Any]) -> dict[str, Any]:
    LOGGER.info(
        "UNIQUE "
        "event_id=%s "
        "key=%s "
        "event_time=%s "
        "window=[%s,%s)",
        event["event_id"],
        event["key"],
        event["event_time"],
        event["window_start"],
        event["window_end"],
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
    output_topic: str,
    group_id: str,
    offset_reset: str,
):
    """Construir la lectura, validación y política temporal."""

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

    windowed_valid = (
        outputs.valid
        | "AssignEventTime"
        >> beam.ParDo(AssignEventTimestampDoFn())
        | "FixedWindows60s"
        >> fixed_window_policy(
            window_seconds=60,
            allowed_lateness_seconds=30,
        )
    )

    deduplicated_valid = deduplicate_by_event_id(
        windowed_valid
    )

    aggregation_ready = (
        deduplicated_valid
        | "AggregationTriggerPolicy"
        >> aggregation_window_policy(
            window_seconds=60,
            allowed_lateness_seconds=30,
        )
    )

    aggregated_quality = aggregate_quality_by_node(
        aggregation_ready
    )

    formatted_quality = (
        aggregated_quality
        | "FormatQualityAggregate"
        >> beam.ParDo(FormatQualityAggregateDoFn())
    )

    
    _ = (
        formatted_quality
        | "LogQualityAggregates"
        >> beam.Map(log_quality_aggregate)
    )


    kafka_ready_quality = (
        formatted_quality
        | "BuildKafkaOutputRecord"
        >> beam.Map(to_kafka_record)
    )

    _ = (
        kafka_ready_quality
        | "WriteQualityToKafka"
        >> WriteToKafka(
            producer_config={
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": "true",
            },
            topic=output_topic,
        )
    )

    _ = (
        outputs.invalid
        | "LogInvalidEvents"
        >> beam.Map(log_invalid_event)
    )

    return formatted_quality, outputs.invalid


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

    parser.add_argument(
        "--output-topic",
        default=DEFAULT_OUTPUT_TOPIC,
    )

    return parser.parse_known_args()


def log_quality_aggregate(
    result: dict[str, Any],
) -> dict[str, Any]:
    LOGGER.info(
        "AGGREGATE "
        "node=%s "
        "window=[%s,%s) "
        "pane=%s "
        "pane_index=%s "
        "samples=%s "
        "avg_latency_ms=%s "
        "avg_packet_loss_pct=%s "
        "avg_throughput_mbps=%s",
        result["node_id"],
        result["window_start"],
        result["window_end"],
        result["pane_timing"],
        result["pane_index"],
        result["sample_count"],
        result["avg_latency_ms"],
        result["avg_packet_loss_pct"],
        result["avg_throughput_mbps"],
    )

    return result

    

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
        output_topic=args.output_topic,
        group_id=args.group_id,
        offset_reset=args.offset_reset,
    )

    
    LOGGER.info(
        "Starting pipeline "
        "input=%s "
        "output=%s "
        "bootstrap=%s "
        "group=%s",
        args.input_topic,
        args.output_topic,
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