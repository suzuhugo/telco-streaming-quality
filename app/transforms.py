from typing import Any

import apache_beam as beam
from apache_beam import pvalue

from app.contracts import deserialize_event

INVALID_TAG = "invalid"


def _safe_text(value: Any) -> str | None:
    """Convertir un valor a texto para fines de diagnóstico."""

    if value is None:
        return None

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    return str(value)


def _decode_kafka_key(raw_key: bytes | None) -> str:
    """Decodificar y validar la clave física recibida desde Kafka."""

    if raw_key is None:
        raise ValueError("Kafka key ausente")

    if not isinstance(raw_key, bytes):
        raise TypeError("Kafka key debe recibirse como bytes")

    try:
        key = raw_key.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            "Kafka key no contiene UTF-8 válido"
        ) from exc

    if not key:
        raise ValueError("Kafka key vacío")

    return key


class ParseAndValidateDoFn(beam.DoFn):
    """Deserializar y validar mensajes procedentes de Kafka."""

    def process(
        self,
        element: tuple[bytes | None, bytes | None],
    ):
        raw_key, raw_value = element

        try:
            kafka_key = _decode_kafka_key(raw_key)

            if raw_value is None:
                raise ValueError("Kafka value ausente")

            event = deserialize_event(raw_value)

            if kafka_key != event["key"]:
                raise ValueError(
                    f"Kafka key {kafka_key!r} no coincide "
                    f"con event.key {event['key']!r}"
                )

        except (TypeError, ValueError) as exc:
            yield pvalue.TaggedOutput(
                INVALID_TAG,
                {
                    "kafka_key": _safe_text(raw_key),
                    "raw_value": _safe_text(raw_value),
                    "error": str(exc),
                },
            )
            return

        yield event