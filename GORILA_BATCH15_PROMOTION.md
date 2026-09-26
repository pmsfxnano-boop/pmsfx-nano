# Gorila Argentum — Batch 15: Promotion

Fecha: 2026-09-26 UTC
Rama: `gorila-argentum-v0-hardening`

## Estado

**IMPLEMENTADO + PERSISTENTE.**

Batch 15 implementa la compuerta formal de promoción. La compuerta es conservadora: cualquier evidencia obligatoria ausente o cualquier criterio fallido mantiene el predictor en `BLOCKED`.

## Criterios

- OOS accuracy >= 0.55
- Brier skill >= 0
- rank IC >= 0
- CPCV mean return > 0
- PBO <= 0.05
- DSR medio > 0
- execution delta medio >= 0
- stress gate aprobado
- data health aprobada
- validación point-in-time aprobada

La decisión se persiste en `promotion_decisions`.

## Evidencia actualmente evaluada

La evidencia histórica de Batch 10 se registra como referencia y produce **BLOCKED**. Esto coincide con el cierre V2: accuracy H10 ≈ 0.4999, rank IC negativo, DSR medio 0, CPCV mean compound return negativo y stress no aprobado.

Artefacto CPCV:
`sha256:afebda5082e1ab512480183b9db7927e733f12b6c8f2ade1bd89e3fbe8794545`

Snapshot:
`4f5274af78f6de30d8854ae80d6a9e62b2e6cfcd9eeea31c76ab2e8d31949946`

## Invariantes

- `ELIGIBLE` no significa promoción automática.
- No hay ejecución de órdenes.
- El estado de promoción no puede cambiarse por una variable de entorno.
- La compuerta sólo puede derivar una decisión de los datos de evidencia entregados.

## API

- `GET /api/promotion`
- `POST /api/promotion/evaluate`

