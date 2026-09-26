# Gorila Argentum — Batch 12: Dashboard

Fecha: 2026-09-26 UTC
Rama: `gorila-argentum-v0-hardening`

## Estado

**PARCIALMENTE IMPLEMENTADO + CI VALIDADO.**

Batch 12 debe convertir la observabilidad existente en un command center operativo sin convertir el dashboard en un mecanismo de promoción.

## Ejecutado

El dashboard existente ahora incorpora un bloque **DRIFT MONITOR** que:

- consulta `/api/drift?limit=50`;
- muestra el estado más reciente por símbolo/campo;
- expone PSI, KS, mean-shift z y ratio de desviación;
- distingue visualmente `OK`, `WARN` y `ALERT`;
- conserva coupling, market state, data flow y temporal engine.

Se añadió cobertura automatizada para verificar que el dashboard conserva el enlace al monitor Drift.

## Validación

La CI del commit `3dfe7e582657832add5b33a0d4729ed3937d5c00` terminó en `success`, incluyendo tests unitarios y compilación de research scripts.

## No declarado como cerrado

Aún no se declara Batch 12 como E2E cerrado porque el servicio Render todavía no ha ejecutado un deploy que contenga estos commits. El último deploy verificable continúa siendo el commit `2a8aa06a1be9951da0e95a810b68e782b2b4d393`.

Tampoco se ha añadido todavía el Control Room de Batch 13 ni un kill-switch automático.

## Regla de continuidad

No se promueve ninguna señal por el hecho de que el dashboard funcione. Predictor y runtime de investigación mantienen sus compuertas estadísticas independientes.
