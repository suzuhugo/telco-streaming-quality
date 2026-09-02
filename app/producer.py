import argparse
import copy
import json
import random
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from confluent_kafka import Producer

from app.contracts import serialize_event

NODES = ("NODE-01", "NODE-02", "NODE-03")

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "telco.telemetry.v1"


def utc_iso(value: datetime) -> str:
    """Convertir datetime UTC al formato ISO-8601 terminado en Z."""

    return (
        value.astimezone(UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def build_event(
    *,
    node_id: str,
    event_time: datetime,
    sequence: int,
    rng: random.Random,
) -> dict[str, Any]:
    """Construir un evento sintético válido de telemetría."""

    event_id = (
        f"{node_id}-"
        f"{event_time.strftime('%Y%m%dT%H%M%S')}-"
        f"{sequence:05d}"
    )

    return {
        "schema_version": 1,
        "event_id": event_id,
        "key": node_id,
        "event_time": utc_iso(event_time),
        "emitted_at": utc_iso(datetime.now(UTC)),
        "payload": {
            "node_id": node_id,
            "latency_ms": round(rng.uniform(20.0, 80.0), 2),
            "packet_loss_pct": round(rng.uniform(0.0, 3.0), 2),
            "throughput_mbps": round(
                rng.uniform(60.0, 120.0),
                2,
            ),
        },
    }


def generate_events(
    *,
    count: int,
    seed: int,
    start_time: datetime | None = None,
) -> list[dict[str, Any]]:
    """Generar una secuencia reproducible de eventos sintéticos."""

    if count <= 0:
        raise ValueError("count debe ser mayor que cero")

    rng = random.Random(seed)

    if start_time is None:
        start_time = datetime.now(UTC).replace(
            microsecond=0
        )

    events = []

    for index in range(count):
        node_id = NODES[index % len(NODES)]

        event_time = start_time + timedelta(seconds=index)

        event = build_event(
            node_id=node_id,
            event_time=event_time,
            sequence=index + 1,
            rng=rng,
        )

        events.append(event)

    return events


def prepare_delivery_sequence(
    events: list[dict[str, Any]],
    *,
    duplicate_rate: float,
    out_of_order_rate: float,
    seed: int,
) -> list[dict[str, Any]]:
    """Preparar la secuencia de publicación con anomalías controladas."""

    if not 0.0 <= duplicate_rate <= 1.0:
        raise ValueError(
            "duplicate_rate debe estar entre 0 y 1"
        )

    if not 0.0 <= out_of_order_rate <= 1.0:
        raise ValueError(
            "out_of_order_rate debe estar entre 0 y 1"
        )

    rng = random.Random(seed)

    delivery = []

    for event in events:
        delivery.append(copy.deepcopy(event))

        if rng.random() < duplicate_rate:
            delivery.append(copy.deepcopy(event))

    index = 0

    while index < len(delivery) - 1:
        if rng.random() < out_of_order_rate:
            delivery[index], delivery[index + 1] = (
                delivery[index + 1],
                delivery[index],
            )
            index += 2
        else:
            index += 1

    return delivery


def delivery_report(err, msg) -> None:
    """Informar el resultado de la entrega realizada por Kafka."""

    if err is not None:
        print(f"ERROR Kafka: {err}")
        return

    print(
        "DELIVERED "
        f"topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def produce_events(
    events: list[dict[str, Any]],
    *,
    bootstrap_servers: str,
    topic: str,
    rate: float,
) -> None:
    """Publicar eventos en Kafka."""

    if rate <= 0:
        raise ValueError("rate debe ser mayor que cero")

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )

    interval = 1.0 / rate

    for event in events:
        raw_value = serialize_event(event)

        producer.produce(
            topic=topic,
            key=event["key"].encode("utf-8"),
            value=raw_value,
            on_delivery=delivery_report,
        )

        producer.poll(0)

        print(
            "SENT "
            f"event_id={event['event_id']} "
            f"key={event['key']} "
            f"event_time={event['event_time']}"
        )

        time.sleep(interval)

    remaining = producer.flush(timeout=10)

    if remaining:
        raise RuntimeError(
            f"Quedaron {remaining} mensajes sin entregar"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Productor sintético de telemetría de red"
    )

    parser.add_argument(
        "--events",
        type=int,
        default=12,
        help="Cantidad de eventos base a generar",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=2.0,
        help="Eventos publicados por segundo",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Semilla para generación reproducible",
    )

    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=0.0,
        help="Proporción de duplicados entre 0 y 1",
    )

    parser.add_argument(
        "--out-of-order-rate",
        type=float,
        default=0.0,
        help="Proporción de intercambios fuera de orden",
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
        "--dry-run",
        action="store_true",
        help="Generar e imprimir sin publicar en Kafka",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    events = generate_events(
        count=args.events,
        seed=args.seed,
    )

    delivery = prepare_delivery_sequence(
        events,
        duplicate_rate=args.duplicate_rate,
        out_of_order_rate=args.out_of_order_rate,
        seed=args.seed + 1,
    )

    print(
        f"Base events: {len(events)} | "
        f"Messages to deliver: {len(delivery)}"
    )

    if args.dry_run:
        for event in delivery:
            print(
                json.dumps(
                    event,
                    ensure_ascii=False,
                )
            )
        return

    produce_events(
        delivery,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        rate=args.rate,
    )


if __name__ == "__main__":
    main()