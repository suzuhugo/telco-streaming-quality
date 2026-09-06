from typing import Any

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms import trigger, window
from apache_beam.utils.windowed_value import PaneInfoTiming

from app.contracts import deserialize_event, parse_utc

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


class AssignEventTimestampDoFn(beam.DoFn):
    """Asignar al elemento el event_time declarado por el dominio."""

    def process(self, event):
        event_datetime = parse_utc(event["event_time"])

        yield beam.window.TimestampedValue(
            event,
            event_datetime.timestamp(),
        )


def fixed_window_policy(
    *,
    window_seconds: int = 60,
    allowed_lateness_seconds: int = 30,
) -> beam.WindowInto:
    """Crear la política temporal base del proyecto."""

    return beam.WindowInto(
        window.FixedWindows(window_seconds),
        allowed_lateness=allowed_lateness_seconds,
    )


class AddWindowMetadataDoFn(beam.DoFn):
    """Agregar metadatos de ventana para observación y pruebas."""

    def process(
        self,
        event,
        window_param=beam.DoFn.WindowParam,
    ):
        yield {
            **event,
            "window_start": window_param.start.to_utc_datetime(
                has_tz=True
            ).isoformat(),
            "window_end": window_param.end.to_utc_datetime(
                has_tz=True
            ).isoformat(),
        }


def key_by_event_id(event):
    """Usar event_id como clave temporal de deduplicación."""

    return event["event_id"], event


def keep_one_event(element):
    """Conservar una copia de un grupo de eventos duplicados."""

    _, events = element

    return next(iter(events))


def deduplicate_by_event_id(events):
    """Eliminar duplicados por event_id dentro de cada ventana."""

    return (
        events
        | "KeyByEventId"
        >> beam.Map(key_by_event_id)
        | "GroupByEventId"
        >> beam.GroupByKey()
        | "KeepOneEvent"
        >> beam.Map(keep_one_event)
    )


class NetworkQualityCombineFn(beam.CombineFn):
    """Calcular métricas agregadas de calidad de red."""

    def create_accumulator(self):
        return (0, 0.0, 0.0, 0.0)

    def add_input(self, accumulator, event):
        (
            count,
            sum_latency,
            sum_packet_loss,
            sum_throughput,
        ) = accumulator

        payload = event["payload"]

        return (
            count + 1,
            sum_latency + float(payload["latency_ms"]),
            sum_packet_loss
            + float(payload["packet_loss_pct"]),
            sum_throughput
            + float(payload["throughput_mbps"]),
        )

    def merge_accumulators(self, accumulators):
        count = 0
        sum_latency = 0.0
        sum_packet_loss = 0.0
        sum_throughput = 0.0

        for accumulator in accumulators:
            (
                partial_count,
                partial_latency,
                partial_packet_loss,
                partial_throughput,
            ) = accumulator

            count += partial_count
            sum_latency += partial_latency
            sum_packet_loss += partial_packet_loss
            sum_throughput += partial_throughput

        return (
            count,
            sum_latency,
            sum_packet_loss,
            sum_throughput,
        )

    def extract_output(self, accumulator):
        (
            count,
            sum_latency,
            sum_packet_loss,
            sum_throughput,
        ) = accumulator

        if count == 0:
            return {
                "sample_count": 0,
                "avg_latency_ms": 0.0,
                "avg_packet_loss_pct": 0.0,
                "avg_throughput_mbps": 0.0,
            }

        return {
            "sample_count": count,
            "avg_latency_ms": round(
                sum_latency / count,
                2,
            ),
            "avg_packet_loss_pct": round(
                sum_packet_loss / count,
                2,
            ),
            "avg_throughput_mbps": round(
                sum_throughput / count,
                2,
            ),
        }


def key_by_node_id(event):
    """Usar node_id como clave para la agregación."""

    return event["payload"]["node_id"], event


def aggregate_quality_by_node(events):
    """Agregar métricas de calidad por nodo y ventana."""

    return (
        events
        | "KeyByNodeId"
        >> beam.Map(key_by_node_id)
        | "CombineQualityByNode"
        >> beam.CombinePerKey(NetworkQualityCombineFn())
    )


class FormatQualityAggregateDoFn(beam.DoFn):
    """Convertir el agregado en un registro de salida."""

    def process(
        self,
        element,
        window_param=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam,
    ):
        node_id, metrics = element

        yield {
            "schema_version": 1,
            "metric_type": "network_quality",
            "node_id": node_id,
            "window_start": (
                window_param.start
                .to_utc_datetime(has_tz=True)
                .isoformat()
            ),
            "window_end": (
                window_param.end
                .to_utc_datetime(has_tz=True)
                .isoformat()
            ),
            "pane_index": pane_info.index,
            "pane_timing": PaneInfoTiming.to_string(
                pane_info.timing
            ),
            **metrics,
        }


def aggregation_window_policy(
    *,
    window_seconds: int = 60,
    allowed_lateness_seconds: int = 30,
) -> beam.WindowInto:
    """Configurar triggers y acumulación para los agregados."""

    return beam.WindowInto(
        window.FixedWindows(window_seconds),
        trigger=trigger.AfterWatermark(
            late=trigger.AfterCount(1),
        ),
        allowed_lateness=allowed_lateness_seconds,
        accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
    )
