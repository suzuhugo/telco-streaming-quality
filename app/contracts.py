import json
import math
from datetime import datetime
from typing import Any


def parse_utc(raw_value: str) -> datetime:
    """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""

    if not isinstance(raw_value, str) or not raw_value.endswith("Z"):
        raise ValueError(
            f"El timestamp debe ser UTC y terminar en 'Z': {raw_value!r}"
        )

    try:
        return datetime.fromisoformat(
            raw_value.removesuffix("Z") + "+00:00"
        )
    except ValueError as exc:
        raise ValueError(
            f"Timestamp ISO-8601 inválido: {raw_value!r}"
        ) from exc

def _is_number(value: Any) -> bool:
    """Comprobar que un valor sea numérico, excluyendo booleanos."""

    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )

def validate_event(event: dict[str, Any]) -> None:
    """Validar el contrato de un evento de telemetría.

    No retorna un valor si el evento es válido.
    Lanza ValueError si encuentra una o más inconsistencias.
    """

    if not isinstance(event, dict):
        raise TypeError("El evento debe ser un objeto JSON")

    errors: list[str] = []

    if event.get("schema_version") != 1:
        errors.append("schema_version debe ser 1")

    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        errors.append("event_id debe ser un string no vacío")

    key = event.get("key")
    if not isinstance(key, str) or not key.strip():
        errors.append("key debe ser un string no vacío")

    try:
        parse_utc(event.get("event_time"))
    except ValueError as exc:
        errors.append(f"event_time inválido: {exc}")

    try:
        parse_utc(event.get("emitted_at"))
    except ValueError as exc:
        errors.append(f"emitted_at inválido: {exc}")

    payload = event.get("payload")

    if not isinstance(payload, dict):
        errors.append("payload debe ser un objeto")
    else:
        node_id = payload.get("node_id")

        if not isinstance(node_id, str) or not node_id.strip():
            errors.append("payload.node_id debe ser un string no vacío")

        if (
            isinstance(key, str)
            and isinstance(node_id, str)
            and key != node_id
        ):
            errors.append("key debe coincidir con payload.node_id")

        latency = payload.get("latency_ms")
        if not _is_number(latency) or latency < 0:
            errors.append("payload.latency_ms debe ser un número >= 0")

        loss = payload.get("packet_loss_pct")
        if not _is_number(loss) or not 0 <= loss <= 100:
            errors.append(
                "payload.packet_loss_pct debe estar entre 0 y 100"
            )

        throughput = payload.get("throughput_mbps")
        if not _is_number(throughput) or throughput < 0:
            errors.append(
                "payload.throughput_mbps debe ser un número >= 0"
            )

    if errors:
        raise ValueError("; ".join(errors))


def serialize_event(event: dict[str, Any]) -> bytes:
    """Validar y serializar un evento a JSON UTF-8."""

    validate_event(event)

    return json.dumps(
        event,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def deserialize_event(raw_value: bytes | str) -> dict[str, Any]:
    """Deserializar JSON y validar el contrato del evento."""

    if isinstance(raw_value, bytes):
        try:
            raw_value = raw_value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(
                "El evento no contiene UTF-8 válido"
            ) from exc

    if not isinstance(raw_value, str):
        raise TypeError(
            "El evento debe recibirse como bytes o string"
        )

    try:
        event = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError("JSON inválido") from exc

    if not isinstance(event, dict):
        raise TypeError(
            "El JSON raíz debe ser un objeto"
        )

    validate_event(event)

    return event