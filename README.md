# Monitoreo en tiempo real de calidad de una red de telecomunicaciones

Proyecto integrador de la asignatura Streaming de datos y sus aplicaciones.

## Objetivo

Implementar un pipeline end-to-end que genere telemetría sintética de red,
publique eventos en Apache Kafka, procese métricas mediante Apache Beam
utilizando tiempo de evento y ventanas, y publique resultados agregados
consumibles.

## Arquitectura

Productor sintético → Kafka → Apache Beam → Kafka → Consumidor

## Infraestructura Kafka

El proyecto utiliza Apache Kafka 4.3.1 ejecutado localmente mediante
Docker Compose en modo KRaft con un único nodo.

### Iniciar Kafka

```bash
docker compose up -d
```

## Contrato de eventos

Los eventos de telemetría utilizan JSON y actualmente corresponden a
`schema_version = 1`.

Ejemplo:

```json
{
  "schema_version": 1,
  "event_id": "NODE-01-20260901T183405-001",
  "key": "NODE-01",
  "event_time": "2026-09-01T18:34:05Z",
  "emitted_at": "2026-09-01T18:34:06Z",
  "payload": {
    "node_id": "NODE-01",
    "latency_ms": 32.5,
    "packet_loss_pct": 0.7,
    "throughput_mbps": 92.4
  }
}
```

### Campos

- `event_id`: identidad estable del evento y futura base para deduplicación.
- `key`: clave de negocio y particionamiento Kafka; coincide con `node_id`.
- `event_time`: momento UTC en que ocurrió la medición.
- `emitted_at`: momento UTC en que el productor publicó el evento.
- `schema_version`: versión del contrato.
- `payload`: mediciones del nodo.

### Reglas de validación

- Los timestamps deben expresarse en UTC y terminar en `Z`.
- `latency_ms` debe ser mayor o igual a cero.
- `packet_loss_pct` debe encontrarse entre 0 y 100.
- `throughput_mbps` debe ser mayor o igual a cero.
- `key` debe coincidir con `payload.node_id`.


## Productor sintético

El productor genera telemetría reproducible para tres nodos:

- `NODE-01`
- `NODE-02`
- `NODE-03`

Las métricas generadas son latencia, pérdida de paquetes y throughput.

### Ejecución normal

```bash
uv run python -m app.producer --events 12 --rate 2
```

### Generación reproducible

```bash
uv run python -m app.producer --events 12 --rate 2 --seed 42
```

### Simular duplicados

```bash
uv run python -m app.producer --events 12 --rate 2 --duplicate-rate 0.20
```

### Simular eventos fuera de orden

```bash
uv run python -m app.producer --events 12 --rate 2 --out-of-order-rate 0.20
```

### Ejecución sin Kafka

```bash
uv run python -m app.producer --events 6 --dry-run
```

El productor utiliza `node_id` como clave Kafka y publica en
`telco.telemetry.v1`.

Los duplicados deliberados conservan el mismo `event_id`. El desorden se
simula alterando el orden de publicación sin modificar el `event_time`.


## Pipeline Beam

El pipeline consume los eventos desde `telco.telemetry.v1` mediante
Apache Beam KafkaIO.

En esta etapa se realizan:

- lectura desde Kafka;
- deserialización JSON;
- validación del contrato;
- validación de consistencia entre Kafka key y `event.key`;
- separación lógica entre eventos válidos e inválidos.

Los eventos inválidos se registran en logs y no detienen el pipeline.

### Ejecutar el pipeline

```bash
uv run python -m app.pipeline \
  --group-id telco-quality-pipeline-v1 \
  --offset-reset latest
```

El pipeline utiliza `DirectRunner` en modo streaming.

### Producir eventos

En otra terminal:

```bash
uv run python -m app.producer --events 12 --rate 2 --seed 42
```

### Replay

Para volver a procesar el historial se puede utilizar un consumer group
nuevo con `earliest`:

```bash
uv run python -m app.pipeline \
  --group-id telco-quality-replay-1 \
  --offset-reset earliest
```

En esta fase el campo `event_time` se valida, pero todavía no se utiliza
como timestamp Beam. La asignación explícita de tiempo de evento se
realiza en la siguiente etapa del pipeline.

## Tiempo de evento y ventanas

El pipeline utiliza `event_time` como timestamp lógico de cada medición.

`event_time` representa cuándo ocurrió la medición en el dominio de red,
mientras que `emitted_at` representa cuándo fue publicada por el productor.

### Ventana

Se utiliza una ventana fija de 60 segundos:

```text
[start, end)
```

Por ejemplo, un evento con:

```text
event_time = 2026-09-03T18:34:25Z
```

pertenece a:

```text
[2026-09-03T18:34:00Z, 2026-09-03T18:35:00Z)
```

La asignación se realiza mediante el `event_time` y no mediante el orden
de llegada a Kafka.

### Desorden

El productor puede alterar el orden de publicación sin modificar
`event_time`, permitiendo demostrar que eventos fuera de orden continúan
asignándose a la ventana temporal correcta.

### Lateness

La política de ventana declara inicialmente:

- ventana fija: 60 segundos;
- allowed lateness: 30 segundos.

La semántica completa de eventos tardíos, triggers y panes se demostrará
junto con la agregación incremental.

## Estado

Proyecto en desarrollo.