# Monitoreo en tiempo real de calidad de una red de telecomunicaciones

Proyecto integrador de la asignatura Streaming de datos y sus aplicaciones.

## Objetivo

Implementar un pipeline end-to-end que genere telemetría sintética de red,
publique eventos en Apache Kafka, procese métricas mediante Apache Beam
utilizando tiempo de evento y ventanas, y publique resultados agregados
consumibles.

## Arquitectura

Productor sintético → Kafka → Apache Beam → Kafka → Consumidor

## Estado

Proyecto en desarrollo.