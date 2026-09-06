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


## Deduplicación

Los eventos se deduplican mediante `event_id` después de asignar
`event_time` y la ventana temporal.

Dentro de cada ventana, el pipeline:

```text
evento
  ↓
(event_id, evento)
  ↓
GroupByKey
  ↓
conserva una copia
```

El horizonte de deduplicación queda acotado por la ventana y su política
de lateness.

Los duplicados deliberados producidos para las pruebas conservan el mismo
`event_id`, `event_time` y `payload`, por lo que representan reintentos del
mismo hecho.

### Demostración

```bash
uv run python -m app.producer \
  --events 6 \
  --rate 2 \
  --seed 42 \
  --duplicate-rate 1.0
```

En este ejemplo se generan 6 hechos base pero se publican 12 mensajes.
La salida lógica del pipeline contiene únicamente los 6 `event_id`
distintos.

### Limitación

Se asume que un `event_id` identifica de forma estable un único hecho. Dos
payloads diferentes con el mismo `event_id` se consideran una violación
del contrato de origen.


## Agregación de calidad de red

Después de validar, asignar tiempo de evento, aplicar ventanas y eliminar
duplicados, el pipeline agrega las mediciones por `node_id`.

La agregación utiliza `CombinePerKey` con un `CombineFn` incremental.

### Métricas

Para cada nodo y ventana se calculan:

- `sample_count`
- `avg_latency_ms`
- `avg_packet_loss_pct`
- `avg_throughput_mbps`

Ejemplo de salida:

```json
{
  "schema_version": 1,
  "metric_type": "network_quality",
  "node_id": "NODE-01",
  "window_start": "2026-09-05T16:00:00+00:00",
  "window_end": "2026-09-05T16:01:00+00:00",
  "sample_count": 4,
  "avg_latency_ms": 34.25,
  "avg_packet_loss_pct": 0.82,
  "avg_throughput_mbps": 91.60
}
```

`CombineFn` mantiene acumuladores parciales de cantidad y sumas y permite
que Beam combine resultados parciales sin materializar todas las mediciones
de una clave.

Los duplicados son eliminados antes de la agregación, por lo que un retry
con el mismo `event_id` no incrementa `sample_count` ni modifica los
promedios.


## Triggers, lateness y panes

La política de agregación utiliza:

- ventana fija de 60 segundos;
- `AfterWatermark` para el pane ON_TIME;
- `AfterCount(1)` para cada revisión LATE;
- `allowed_lateness = 30` segundos;
- modo `ACCUMULATING`;
- no se utilizan panes EARLY.

El pane ON_TIME se emite cuando el watermark supera el final de la
ventana.

Si posteriormente llega un evento cuyo `event_time` pertenece a esa
ventana y todavía se encuentra dentro del margen de 30 segundos, se emite
un pane LATE.

En modo ACCUMULATING, el pane tardío representa el resultado completo
revisado y no solamente la contribución del evento nuevo.

Ejemplo:

```text
ON_TIME
sample_count = 2
avg_latency_ms = 30

LATE
sample_count = 3
avg_latency_ms = 40
```

Los panes de una misma combinación `node_id + ventana` deben interpretarse
como revisiones del mismo resultado lógico.

### Límite de la implementación

La deduplicación utiliza agrupación por `event_id` dentro de ventanas para
mantener una solución simple. No se afirma una garantía stateful global
para el caso combinado de un duplicado que reaparece en revisiones tardías.

Tampoco se afirma exactamente-once end-to-end.



## Estado

Proyecto en desarrollo.