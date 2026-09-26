# Gorila Argentum — Estado de implementación y cierre técnico

Fecha: 2026-09-26 UTC
Rama: `gorila-argentum-v0-hardening`
Último checkpoint de CI de ingeniería: `aec9102f3e4c2b95ecdac5343aa4e10ca472d8b8`

## Estado por lote

| Batch | Estado | Evidencia / bloqueo |
|---|---|---|
| 00 Foundation | Cerrado históricamente | Continuidad recuperada |
| 01 Data Fabric | Cerrado históricamente | Persistencia/normalización existentes |
| 02 Fast Ingestion | Cerrado históricamente | Ingestión concurrente existente |
| 03 Argentina Market State | Cerrado históricamente | State endpoint existente |
| 04 Matriz de Acoplamiento Dinámico | Cerrado históricamente | Coupling endpoint existente |
| 05 Feature Engine | Cerrado históricamente | Features existentes |
| 06 Regime Engine | Cerrado históricamente | Tests existentes |
| 07 Prediction V0 | Implementado; promoción bloqueada | Evidencia V2 no supera gate |
| 08 Temporal Engine | Cerrado históricamente | Timing window existente |
| 09 Validation | Cerrado | WFO/search-aware/CPCV hardening |
| 10 Outcome Engine | Cerrado | Evidencia V2 archivada; predictor bloqueado |
| 11 Drift | Implementado + persistente | Data drift + prediction drift + outcome drift |
| 12 Dashboard | Implementado | E2E Render pendiente |
| 13 Control Room | Implementado + persistente | Circuit breaker de investigación activo |
| 14 Shadow | Implementado + persistente | Settlement point-in-time; E2E Render pendiente |
| 15 Promotion | Implementado + persistente | Gate actual BLOCKED |
| 16 Continuous Learning | Implementado | Candidato, recalibración y runtime tick; cron Render pendiente |

## Cadena operativa

ingest → deduplicated observations → market state → coupling → features → regime → prediction/timing → Shadow → outcome → Drift → Control Room → Promotion Gate → Continuous Learning → recalibration candidate → audit

## Invariantes

- Runtime conceptual: `RESEARCH`.
- Trading execution: deshabilitado.
- Automatic promotion: deshabilitada.
- Predictor V2: `BLOCKED`.
- Storage durability: obligatoria para learning/settlement operacional.
- Shadow settlement: sólo cuando `event_time` alcanza el horizonte.
- Continuous Learning: genera candidatos; no reemplaza automáticamente el modelo.
- Recalibración: candidata y persistida; aplicación automática deshabilitada.
- Data/prediction/outcome drift: monitorizados.
- Circuit breaker: `HALTED` ante almacenamiento no durable, drift crítico o fallos de fuente/modelo.
- Multi-source duplicate event times: deduplicados antes de construir series.

## CI verificada

En el checkpoint `aec9102f…`:
- Gorila Argentum Hardening CI: **success**
- Gorila Argentum Learning Integrity: **success**

Las validaciones científicas largas (True CPCV y Final Validation V2) se ejecutan por separado y no se sustituyen por el estado del CI de software.

## Scheduler

El workflow GitHub provisional fue retirado porque no producía una ejecución utilizable para este branch. La lógica recurrente quedó encapsulada en:

`scripts/gorila_runtime_tick.py`

El tick exige Postgres y ejecuta learning, settlement, evaluación de Promotion, recalibración candidata y Audit. Queda preparado para un Render Cron Job cuando el runtime esté desplegado y el workspace de Render haya sido autorizado.

## Bloqueo externo real

No se declara E2E de producción porque el nuevo commit todavía no ha sido desplegado/validado en Render. La herramienta exige confirmar explícitamente el workspace antes de operar sobre el servicio. El último deploy históricamente verificado sigue siendo:

`dep-dargqp7pn0mc73cgi3kg` → commit `2a8aa06a1be9951da0e95a810b68e782b2b4d393`.

## Regla científica

El bloqueo de Promotion no se relaja por infraestructura nueva. Batch 10 continúa siendo la referencia estadística vigente hasta producir evidencia OOS nueva y reproducible que pase todos los gates.
