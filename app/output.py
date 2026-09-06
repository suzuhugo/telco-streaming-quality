import json
from typing import Any


def make_aggregate_key(
    result: dict[str, Any],
) -> str:
    """Construir la clave lógica estable del agregado."""

    validate_quality_aggregate(result)

    return (
        f"{result['metric_type']}|"
        f"{result['node_id']}|"
        f"{result['window_start']}"
    )

REQUIRED_OUTPUT_FIELDS = {
    "schema_version",
    "metric_type",
    "node_id",
    "window_start",
    "window_end",
    "pane_index",
    "pane_timing",
    "sample_count",
    "avg_latency_ms",
    "avg_packet_loss_pct",
    "avg_throughput_mbps",
}

def validate_quality_aggregate(
    result: dict[str, Any],
) -> None:
    """Validar el contrato mínimo del agregado de salida."""

    if not isinstance(result, dict):
        raise TypeError(
            "El agregado debe ser un diccionario"
        )

    missing = REQUIRED_OUTPUT_FIELDS - result.keys()

    if missing:
        names = ", ".join(sorted(missing))
        raise ValueError(
            f"Faltan campos de salida: {names}"
        )

    if result["schema_version"] != 1:
        raise ValueError(
            "schema_version debe ser 1"
        )

    if result["metric_type"] != "network_quality":
        raise ValueError(
            "metric_type debe ser network_quality"
        )

    node_id = result["node_id"]

    if not isinstance(node_id, str) or not node_id:
        raise ValueError(
            "node_id debe ser un string no vacío"
        )

    if (
        not isinstance(result["pane_index"], int)
        or result["pane_index"] < 0
    ):
        raise ValueError(
            "pane_index debe ser un entero >= 0"
        )

    pane_timing = result["pane_timing"]

    if not isinstance(pane_timing, str) or not pane_timing:
        raise ValueError(
            "pane_timing debe ser un string no vacío"
        )

    if (
        not isinstance(result["sample_count"], int)
        or result["sample_count"] < 0
    ):
        raise ValueError(
            "sample_count debe ser un entero >= 0"
        )

def serialize_quality_aggregate(
    result: dict[str, Any],
) -> bytes:
    """Serializar un agregado validado a JSON UTF-8."""

    validate_quality_aggregate(result)

    return json.dumps(
        result,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

def to_kafka_record(
    result: dict[str, Any],
) -> tuple[bytes, bytes]:
    """Convertir un agregado en key/value para Kafka."""

    key = make_aggregate_key(result).encode(
        "utf-8"
    )

    value = serialize_quality_aggregate(result)

    return key, value

def materialize_upserts(
    results: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Materializar la última revisión de cada entidad lógica."""

    store: dict[str, dict[str, Any]] = {}

    for result in results:
        key = make_aggregate_key(result)

        store[key] = result

    return store

