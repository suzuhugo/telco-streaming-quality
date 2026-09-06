import argparse
import json
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from app.output import (
    make_aggregate_key,
    validate_quality_aggregate,
)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "telco.quality.v1"
DEFAULT_GROUP_ID = "telco-quality-view-v1"

def deserialize_quality_aggregate(
    raw_value: bytes | None,
) -> dict[str, Any]:
    """Deserializar y validar un agregado recibido desde Kafka."""

    if raw_value is None:
        raise ValueError("Kafka value ausente")

    if not isinstance(raw_value, bytes):
        raise TypeError(
            "Kafka value debe recibirse como bytes"
        )

    try:
        text = raw_value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            "Kafka value no contiene UTF-8 válido"
        ) from exc

    try:
        result = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "Kafka value no contiene JSON válido"
        ) from exc

    if not isinstance(result, dict):
        raise TypeError(
            "El agregado JSON debe ser un objeto"
        )

    validate_quality_aggregate(result)

    return result

def decode_kafka_key(
    raw_key: bytes | None,
) -> str:
    """Decodificar la clave lógica recibida desde Kafka."""

    if raw_key is None:
        raise ValueError("Kafka key ausente")

    if not isinstance(raw_key, bytes):
        raise TypeError(
            "Kafka key debe recibirse como bytes"
        )

    try:
        key = raw_key.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            "Kafka key no contiene UTF-8 válido"
        ) from exc

    if not key:
        raise ValueError("Kafka key vacío")

    return key

def apply_upsert(
    current_view: dict[str, dict[str, Any]],
    *,
    kafka_key: str,
    aggregate: dict[str, Any],
) -> str:
    """Aplicar una revisión del agregado a la vista materializada."""

    expected_key = make_aggregate_key(aggregate)

    if kafka_key != expected_key:
        raise ValueError(
            f"Kafka key {kafka_key!r} no coincide "
            f"con la clave lógica esperada {expected_key!r}"
        )

    operation = (
        "UPDATE"
        if kafka_key in current_view
        else "INSERT"
    )

    current_view[kafka_key] = aggregate

    return operation

def print_current_view(
    current_view: dict[str, dict[str, Any]],
) -> None:
    """Mostrar la vista materializada actual."""

    print()
    print(
        f"{'NODE':<10} "
        f"{'WINDOW_START':<27} "
        f"{'PANE':<10} "
        f"{'COUNT':>5} "
        f"{'LATENCY':>10} "
        f"{'LOSS':>8} "
        f"{'THROUGHPUT':>12}"
    )

    print("-" * 95)

    ordered = sorted(
        current_view.values(),
        key=lambda item: (
            item["window_start"],
            item["node_id"],
        ),
    )

    for result in ordered:
        print(
            f"{result['node_id']:<10} "
            f"{result['window_start']:<27} "
            f"{result['pane_timing']:<10} "
            f"{result['sample_count']:>5} "
            f"{result['avg_latency_ms']:>10.2f} "
            f"{result['avg_packet_loss_pct']:>8.2f} "
            f"{result['avg_throughput_mbps']:>12.2f}"
        )

    print()

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Consumidor de agregados de calidad de red"
        )
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
    )

    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
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

    return parser.parse_args()

def consume() -> None:
    args = parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.group_id,
            "auto.offset.reset": args.offset_reset,
            "enable.auto.commit": True,
        }
    )

    current_view: dict[
        str,
        dict[str, Any],
    ] = {}

    consumer.subscribe([args.topic])

    print(
        "Consumer iniciado "
        f"topic={args.topic} "
        f"group={args.group_id} "
        f"offset_reset={args.offset_reset}"
    )

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if (
                    message.error().code()
                    == KafkaError._PARTITION_EOF
                ):
                    continue

                raise KafkaException(
                    message.error()
                )

            try:
                kafka_key = decode_kafka_key(
                    message.key()
                )

                aggregate = (
                    deserialize_quality_aggregate(
                        message.value()
                    )
                )

                operation = apply_upsert(
                    current_view,
                    kafka_key=kafka_key,
                    aggregate=aggregate,
                )

            except (TypeError, ValueError) as exc:
                print(
                    "INVALID "
                    f"partition={message.partition()} "
                    f"offset={message.offset()} "
                    f"error={exc}"
                )
                continue

            print(
                f"{operation} "
                f"key={kafka_key} "
                f"pane={aggregate['pane_timing']} "
                f"pane_index={aggregate['pane_index']}"
            )

            print_current_view(current_view)

    except KeyboardInterrupt:
        print()
        print("Consumer detenido por el usuario.")

    finally:
        consumer.close()


if __name__ == "__main__":
    consume()