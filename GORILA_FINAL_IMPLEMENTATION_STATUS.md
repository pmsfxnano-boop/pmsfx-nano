# Gorila Argentum — Estado final de implementación de lotes

Fecha: 2026-09-26 UTC
Rama: gorila-argentum-v0-hardening

## Estado por lote

| Batch | Estado de implementación | Evidencia / bloqueo |
|---|---|---|
| 00 Foundation | Cerrado históricamente | Continuidad recuperada |
| 01 Data Fabric | Cerrado históricamente | Persistencia/normalización existentes |
| 02 Fast Ingestion | Cerrado históricamente | Ingestión concurrente existente |
| 03 Argentina Market State | Cerrado históricamente | State endpoint existente |
| 04 Matriz de Acoplamiento Dinámico | Cerrado históricamente | Coupling endpoint existente |
| 05 Feature Engine | Cerrado históricamente | Features existentes |
| 06 Regime Engine | Cerrado históricamente | Tests existentes |
| 07 Prediction V0 | Cerrado como implementación; promoción bloqueada | Evidencia V2 no supera gate |
| 08 Temporal Engine | Cerrado históricamente | Timing window existente |
| 09 Validation | Cerrado históricamente | WFO/search-aware/CPCV hardening existente |
| 10 Outcome Engine | Cerrado | Evidencia V2 archivada; predictor bloqueado |
| 11 Drift | Implementado + persistente | CI histórica; E2E Render no verificable |
| 12 Dashboard | Implementado | E2E Render no verificable |
| 13 Control Room | Implementado + persistente | E2E Render no verificable |
| 14 Shadow | Implementado + persistente + settlement point-in-time | E2E Render no verificable |
| 15 Promotion | Implementado + persistente | Gate actual BLOCKED por evidencia V2 |
| 16 Continuous Learning | Implementado como candidatos controlados | Scheduler/deploy runtime no verificable |

## Cadena actual

ingest → deduplicated observations → market state → coupling → features → regime → prediction/timing → Shadow → outcome → Drift → Control Room → Promotion Gate → Continuous Learning

## Invariantes

- Runtime: RESEARCH.
- Trading execution: deshabilitado.
- Automatic promotion: deshabilitada.
- Predictor V2: BLOCKED.
- Shadow settlement: sólo con observación cuyo event_time alcanza el horizonte.
- Continuous Learning: genera candidatos; no reemplaza el modelo vigente.
- Multi-source duplicate event times: deduplicados antes de construir series.

## Único bloqueo externo de cierre operacional

Los commits están en GitHub, pero la herramienta Render exige que el workspace sea confirmado antes de operar/validar el servicio. En esta sesión no se ha autorizado la selección de workspace. Por eso no se declara E2E de producción ni se inventa un deploy.

El servicio conocido sigue siendo `gorila-argentum-research`, y el último deploy verificable históricamente sigue apuntando al commit `2a8aa06a1be9951da0e95a810b68e782b2b4d393`.