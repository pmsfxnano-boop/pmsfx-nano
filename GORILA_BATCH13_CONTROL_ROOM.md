# Gorila Argentum — Batch 13: Control Room

Fecha: 2026-09-26 UTC
Rama: `gorila-argentum-v0-hardening`

## Estado

**IMPLEMENTADO + TEST CONFIGURADO + CI PENDIENTE DE OBSERVACIÓN.**

Batch 13 añade una capa de control/observabilidad que expone el estado operativo sin convertirlo en un mecanismo de promoción automática.

## Ejecutado

Se añadió `gorila_argentum/control.py` con una representación explícita de:

- modo de runtime (`RESEARCH`);
- backend de almacenamiento efectivo (Postgres o fallback SQLite);
- estado de la compuerta de promoción del predictor;
- motivo configurable de la compuerta;
- capacidades de monitorización implementadas y todavía no implementadas;
- salud de fuentes;
- últimos snapshots de Drift y alertas `WARN/ALERT`.

Se añadió:

`GET /api/control`

El dashboard incorpora **CONTROL ROOM — BATCH 13** y muestra runtime, storage, promoción, alertas de Drift, prediction drift y kill-switch.

## Regla de seguridad

El control room **no promueve** modelos, **no ejecuta trading** y **no activa** un kill-switch que todavía no existe.

El estado de promoción no es configurable por runtime: el Control Room conserva `BLOCKED` hasta que una implementación de gate científico lo derive explícitamente.

## Validación

Se añadió `tests/test_control.py` para verificar:

- runtime en `RESEARCH`;
- promoción por defecto `BLOCKED`;
- ausencia de promoción automática;
- Drift de distribución implementado;
- kill-switch automático no declarado como implementado;
- propagación de alertas Drift persistidas.

La CI debe incluir este test junto con el conjunto existente de hardening.

## No declarado como cerrado

El servicio Render todavía no ha sido actualizado con estos commits. Por lo tanto, este documento no declara E2E de producción.

Tampoco declara implementados prediction drift, realized-vs-predicted drift, recalibración automática ni kill-switch automático.
